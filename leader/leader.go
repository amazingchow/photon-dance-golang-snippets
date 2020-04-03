package leader

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	leaderelection "github.com/Comcast/go-leaderelection"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	_BaseNode       = "/election"
	_LeaderElection = "leader"
)

func createZkNodes(conn *zk.Conn, path string) {
	_, err := conn.Create(path,
		[]byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		log.Fatalf("Error creating the election node (%s): %v",
			path, err)
	}
}

func ElectLeader(zkEndpoints []string, node string) {
	conn, session, err := zk.Connect(zkEndpoints, 10*time.Second)
	if err != nil {
		exit(err)
	}

	createZkNodes(conn, _BaseNode)
	electionNode := _BaseNode + "/" + _LeaderElection
	createZkNodes(conn, electionNode)

	elector, err := leaderelection.NewElection(conn, electionNode, node)
	if err != nil {
		exit(err)
	}

	// `leak` the zk connection
	go func() {
		for ev := range session {
			if ev.State == zk.StateExpired || ev.State == zk.StateAuthFailed {
				elector.Resign()
				exit(errors.New("bad zookeeper session"))
			}
		}
		elector.Resign()
		conn.Close()
		exit(errors.New("zookeeper session closed"))
	}()

	go elector.ElectLeader()

	for status := range elector.Status() {
		if status.Err != nil {
			elector.Resign()
			exit(status.Err)
		}
		fmt.Printf("candidate %s received status message: <' %v '>\n", node, status)
		if status.Role == leaderelection.Leader {
			fmt.Printf("leader elected: %s\n", node)
		}
	}

	elector.Resign()
	exit(errors.New("election channel closed"))
}

func exit(err error) {
	fmt.Println(err)
	os.Exit(1)
}
