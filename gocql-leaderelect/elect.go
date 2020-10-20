package gocqlleaderelect

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog/log"
)

const (
	_StatusAcquiring = 0
	_StatusMaster    = 1
	_StatusResigned  = 2

	_DBTimeout     = 2 * time.Second
	_DefaultTTLSec = 10
)

// Role 节点角色
type Role int

const (
	// Follower 从节点
	Follower Role = 0
	// Leader 主节点
	Leader = 1
)

// Status 节点状态
type Status struct {
	LeaderID      string
	LeaderAddress string
	Role          Role
}

// Elector 用于选主
type Elector struct {
	cfg     *ElectorCfg
	session *gocql.Session

	status int32

	ev   chan Status
	dead chan bool
}

// ElectorCfg 选主配置
type ElectorCfg struct {
	Hosts               []string // cassandra服务节点列表
	NodeID              string
	AdvertiseAddress    string
	HeartbeatTimeoutSec int
	Keyspace            string
	TableName           string
	ResourceName        string
}

// NewConfig 生成默认配置.
func NewConfig(node string, resource string) *ElectorCfg {
	return &ElectorCfg{
		NodeID:              node,
		HeartbeatTimeoutSec: 10,
		Keyspace:            "leader_elect_test",
		TableName:           "leader_elect",
		ResourceName:        resource,
	}
}

// NewElector 返回Elector实例.
func NewElector(cfg *ElectorCfg) (*Elector, error) {
	if cfg.Keyspace == "" {
		panic("no keyspace")
	}

	clusterCfg := gocql.NewCluster(cfg.Hosts...)
	clusterCfg.ConnectTimeout = _DBTimeout
	clusterCfg.Timeout = _DBTimeout
	clusterCfg.Keyspace = cfg.Keyspace

	session, err := clusterCfg.CreateSession()
	if err != nil {
		return nil, err
	}

	e := Elector{
		cfg:     cfg,
		session: session,
		ev:      make(chan Status, 1000),
		dead:    make(chan bool),
	}
	e.createTable(_DefaultTTLSec)

	return &e, nil
}

func (e *Elector) createTable(ttl int) error {
	cql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		resource_name text PRIMARY KEY,
		leader_id text,
		value text
	) with default_time_to_live = %d`, e.cfg.Keyspace, e.cfg.TableName, ttl)
	return e.session.Query(cql).Exec()
}

// NewElectorWithSession 返回Elector实例.
func NewElectorWithSession(cfg *ElectorCfg, session *gocql.Session) *Elector {
	if cfg.Keyspace == "" {
		panic("no keyspace")
	}
	return &Elector{
		cfg:     cfg,
		session: session,
		ev:      make(chan Status, 1000),
		dead:    make(chan bool),
	}
}

// Start 开始选举.
func (e *Elector) Start() {
	go e.elect()
}

func (e *Elector) elect() {
ELECT_LOOP:
	for {
		s := atomic.LoadInt32(&e.status)
		switch s {
		case _StatusAcquiring:
			{
				status, err := e.createLease()
				if err != nil {
					log.Warn().Err(err).Msg("failed to acquire lease")
					e.sleep(false)
					continue
				}
				e.ev <- status
				if status.Role == Leader {
					if !atomic.CompareAndSwapInt32(&e.status, s, _StatusMaster) {
						continue
					}
				}
				e.sleep(status.Role == Leader)
			}
		case _StatusMaster:
			{
				status, err := e.updateLease()
				if err != nil {
					log.Warn().Err(err).Msg("failed to acquire lease for master, downgrade to follower")
					status = Status{Role: Follower}
				}
				e.ev <- status
				if status.Role != Leader {
					if !atomic.CompareAndSwapInt32(&e.status, s, _StatusAcquiring) {
						continue
					}
				}
				e.sleep(status.Role == Leader)
			}
		case _StatusResigned:
			{
				if err := e.removeLease(); err != nil {
					log.Warn().Err(err).Msg("failed to remove lease")
				}
				break ELECT_LOOP
			}
		default:
			panic("unknown node status")
		}
	}
	log.Info().Msgf("election resigned: %s", e.cfg.NodeID)
	close(e.ev)
	close(e.dead)
}

func (e *Elector) sleep(leader bool) {
	sec := e.cfg.HeartbeatTimeoutSec
	if leader {
		sec = (e.cfg.HeartbeatTimeoutSec - 1) / 2
	}

	deadline := time.Now().Add(time.Duration(sec) * time.Second)
SLEEP_LOOP:
	for {
		s := atomic.LoadInt32(&e.status)
		if s == _StatusResigned {
			break SLEEP_LOOP
		}
		now := time.Now()
		if deadline.Before(now) {
			break SLEEP_LOOP
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *Elector) createLease() (Status, error) {
	cql := fmt.Sprintf(`INSERT INTO %s.%s (resource_name, leader_id, value) VALUES (?,?,?) IF NOT EXISTS`,
		e.cfg.Keyspace, e.cfg.TableName)
	q := e.session.Query(cql, e.cfg.ResourceName, e.cfg.NodeID, e.cfg.AdvertiseAddress)
	defer q.Release()

	var rn, id, val string
	applied, err := q.ScanCAS(&rn, &id, &val)
	if err != nil {
		return Status{}, err
	}
	return e.statusFromDB(applied, id, val), nil
}

func (e *Elector) updateLease() (Status, error) {
	cql := fmt.Sprintf(`UPDATE %s.%s SET leader_id = ?, value = ? WHERE resource_name = ? IF leader_id = ?`,
		e.cfg.Keyspace, e.cfg.TableName)
	q := e.session.Query(cql, e.cfg.NodeID, e.cfg.AdvertiseAddress, e.cfg.ResourceName, e.cfg.NodeID)
	defer q.Release()

	var id, val string
	applied, err := q.ScanCAS(&id, &val)
	if err != nil {
		return Status{}, err
	}
	return e.statusFromDB(applied, id, val), nil
}

func (e *Elector) removeLease() error {
	cql := fmt.Sprintf(`DELETE FROM %s.%s WHERE resource_name = ? IF leader_id = ?`,
		e.cfg.Keyspace, e.cfg.TableName)
	q := e.session.Query(cql, e.cfg.ResourceName, e.cfg.NodeID)
	defer q.Release()

	var id string
	_, err := q.ScanCAS(&id)
	return err
}

func (e *Elector) statusFromDB(applied bool, id string, val string) Status {
	if applied || id == e.NodeID() {
		return Status{
			LeaderID:      e.cfg.NodeID,
			LeaderAddress: e.cfg.AdvertiseAddress,
			Role:          Leader,
		}
	}
	return Status{
		LeaderID:      id,
		LeaderAddress: val,
		Role:          Follower,
	}
}

// NodeID 显示当前选举节点的ID.
func (e *Elector) NodeID() string {
	return e.cfg.NodeID
}

// Resign 退出选举.
func (e *Elector) Resign() {
	atomic.StoreInt32(&e.status, _StatusResigned)
	<-e.dead
}

// Status 用于显示当前选举节点的状态.
func (e *Elector) Status() <-chan Status {
	return e.ev
}
