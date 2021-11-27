package main

import (
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const ZK_ADDR = "localhost"
const ZK_TIMEOUT = 3 * time.Second
const ELECTION_NAMESPACE = "/election"
const TARGET_ZNODE = "/target_znode"

var currentZnodeName string

func main() {
	l := log.New(os.Stdout, "leader-election", log.LstdFlags)
	zookeeper, eventCh, err := connectZk()
	if err != nil {
		l.Panic(err)
	}
	go func() {
		for {
			process(zookeeper, <-eventCh, l)
		}
	}()

	volunteerForLeadership(zookeeper, l)
	electLeader(zookeeper, l)

	// Listen to interrupt signal on channel
	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, os.Interrupt)

	// Shutdown gracefully upon receiving SIGINT
	sig := <-sigChan
	zookeeper.Close()
	l.Println("Received terminate, graceful shutdown", sig)
}

func watchTargetZnode(zookeeper *zk.Conn, l *log.Logger) {
	exists, _, _, err := zookeeper.ExistsW(TARGET_ZNODE)
	if err != nil {
		l.Panic(err)
	}
	if !exists {
		return
	}
	children, _, _, err := zookeeper.ChildrenW(TARGET_ZNODE)
	l.Println("children", children)

}

func electLeader(zookeeper *zk.Conn, l *log.Logger) {
	children, _, err := zookeeper.Children(ELECTION_NAMESPACE)
	if err != nil {
		l.Panic(err)
	}

	sort.Strings(children)
	smallestChild := children[0]

	if smallestChild == currentZnodeName {
		l.Println("I am the leader")
		return
	}

	l.Println("I am not the leader", smallestChild, "is the leader")
}

func volunteerForLeadership(zookeeper *zk.Conn, l *log.Logger) {
	zNodePrefix := ELECTION_NAMESPACE + "/c_"
	zNodeFullPath, err := zookeeper.Create(
		zNodePrefix,
		[]byte{}, zk.FlagEphemeral+zk.FlagSequence,
		zk.WorldACL(zk.PermWrite))
	if err != nil {
		l.Panic(err)
	}

	l.Println("znode name", zNodeFullPath)
	currentZnodeName = strings.Replace(zNodeFullPath, ELECTION_NAMESPACE+"/", "", 1)
}

func connectZk() (*zk.Conn, <-chan zk.Event, error) {
	c, eventCh, err := zk.Connect([]string{ZK_ADDR}, ZK_TIMEOUT)
	if err != nil {
		return nil, nil, err
	}
	return c, eventCh, nil
}

func process(zookeeper *zk.Conn, event zk.Event, l *log.Logger) {
	if event.Type.String() == "EventSession" {
		if event.State == zk.StateConnected {
			l.Println("Successfully connected to zookeeper")
		} else if event.State == zk.StateDisconnected {
			l.Println("Disconnected from zookeeper")
		}
	}

	switch eventType := event.Type.String(); eventType {
	case "EventNodeDeleted":
		l.Println(TARGET_ZNODE, "was deleted")
	case "EventNodeDataChanged":
		l.Println(TARGET_ZNODE, "data changed")
	case "EventNodeCreated":
		l.Println(TARGET_ZNODE, "was created")
	case "EventChildrenChanged":
		l.Println(TARGET_ZNODE, "children changed")
	}

	watchTargetZnode(zookeeper, l)
}
