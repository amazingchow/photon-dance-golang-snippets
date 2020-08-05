package electleader

import (
	"time"

	leaderelection "github.com/Comcast/go-leaderelection"
	"github.com/rs/zerolog/log"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	_ElectionPath = "/election"
	_LeaderPath   = "/leader"

	_ElectionHeartbeat = 10 * time.Second
)

// Elector 用于选主
type Elector struct {
	conn         *zk.Conn
	participator string
	close        chan struct{}
	stop         chan struct{}
	cfg          *ElectorCfg
}

// ElectorCfg 选主配置
type ElectorCfg struct {
	ZKEndpoints       []string `json:"zk_endpoints"`
	ElectionHeartbeat int      `json:"election_heartbeat"`
}

// NewElector 返回Elector实例.
func NewElector(cfg *ElectorCfg) *Elector {
	return &Elector{
		close: make(chan struct{}),
		stop:  make(chan struct{}),
		cfg:   cfg,
	}
}

// ElectLeader 让participator参与选主.
func (e *Elector) ElectLeader(participator string) {
	var heartbeat time.Duration
	if e.cfg.ElectionHeartbeat > 0 {
		heartbeat = time.Duration(e.cfg.ElectionHeartbeat) * time.Second
	} else {
		heartbeat = _ElectionHeartbeat
	}

	conn, sessionEv, err := zk.Connect(e.cfg.ZKEndpoints, heartbeat)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", participator).Msgf("failed to connect to zookeeper cluster (%v)", e.cfg.ZKEndpoints)
	}
	e.conn = conn
	e.participator = participator

	e.prepare()

	candidate, err := leaderelection.NewElection(e.conn, _ElectionPath, e.participator)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msg("failed to start election for candidate")
	}

	go candidate.ElectLeader()

ELECT_LOOP:
	for {
		select {
		case ev := <-sessionEv:
			{
				if ev.State == zk.StateExpired {
					log.Error().Err(zk.ErrSessionExpired).Str("[participator]", e.participator)
					break ELECT_LOOP
				} else if ev.State == zk.StateAuthFailed {
					log.Error().Err(zk.ErrAuthFailed).Str("[participator]", e.participator)
					break ELECT_LOOP
				} else if ev.State == zk.StateUnknown {
					log.Error().Err(zk.ErrUnknown).Str("[participator]", e.participator)
					break ELECT_LOOP
				} else if ev.State.String() == "unknown state" {
					break ELECT_LOOP
				}
				if ev.State == zk.StateDisconnected {
					log.Warn().Str("[participator]", e.participator).Msgf("zookeeper server (%s) state turns into %s", ev.Server, ev.State.String())
				}
				log.Info().Str("[participator]", e.participator).Msgf("zookeeper server (%s) state turns into %s", ev.Server, ev.State.String())
			}
		case status, ok := <-candidate.Status():
			{
				if !ok {
					log.Error().Str("[participator]", e.participator).Msg("election channel has been closed, election will terminate")
					break ELECT_LOOP
				} else if status.Err != nil {
					log.Error().Err(status.Err).Str("[participator]", e.participator).Msg("candidate received election status error")
					break ELECT_LOOP
				} else {
					log.Info().Str("[participator]", e.participator).Msgf("candidate received status message\n\n%v", status.String())
					if status.Role == leaderelection.Leader {
						e.setLeader(e.participator)
						// TODO: do your stuff
						log.Info().Str("[participator]", e.participator).Msg("candidate has been promoted to leader")
					} else if status.Role == leaderelection.Follower {
						e.getLeader()
						// TODO: do your stuff
					}
				}
			}
		case _, ok := <-e.stop:
			{
				if !ok {
					break ELECT_LOOP
				}
			}
		}
	}
	candidate.Resign()
	e.conn.Close()
	e.close <- struct{}{}
}

func (e *Elector) prepare() {
	_, err := e.conn.Create(_ElectionPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to create znode (%s)", _ElectionPath)
	}

	_, err = e.conn.Create(_LeaderPath, []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to create znode (%s)", _ElectionPath)
	}
}

func (e *Elector) setLeader(leader string) {
	_, err := e.conn.Set(_LeaderPath, []byte(leader), -1)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to set value for znode (%s)", _LeaderPath)
	}
}

func (e *Elector) getLeader() string {
	v, _, err := e.conn.Get(_LeaderPath)
	if err != nil {
		log.Fatal().Err(err).Str("[participator]", e.participator).Msgf("failed to get value from znode (%s)", _LeaderPath)
	}

	return string(v)
}

// Close participator主动退出选主.
func (e *Elector) Close() {
	close(e.stop)
	<-e.close
	log.Warn().Str("[participator]", e.participator).Msg("leave")
}
