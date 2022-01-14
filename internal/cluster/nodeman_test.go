package cluster

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
)

func TestNodeManager_Join(t *testing.T) {
	nodeman1 := testNodeManager(t)
	nodeman2 := testNodeManager(t)
	nodeman3 := testNodeManager(t)
	nodes := make([]*nodeInfo, 0)

	// node-1 start
	nodeman1.OnJoinFunc(func(ni *nodeInfo) {
		nodes = append(nodes, ni)
	})
	assert.Nil(t, nodeman1.ListenAndServe())
	time.Sleep(10 * time.Millisecond)

	// node-2 join to node-1
	nodeman2.OnJoinFunc(func(ni *nodeInfo) {
		nodes = append(nodes, ni)
	})
	assert.Nil(t, nodeman2.ListenAndServe())
	err := tryJoin(func() error {
		_, err := nodeman2.Join([]string{nodeman1.Addr()})
		return err
	})
	assert.Nil(t, err)
	time.Sleep(10 * time.Millisecond)

	// node-3 join to node-1
	nodeman3.OnJoinFunc(func(ni *nodeInfo) {
		nodes = append(nodes, ni)
	})
	assert.Nil(t, nodeman3.ListenAndServe())
	err = tryJoin(func() error {
		_, err := nodeman3.Join([]string{nodeman1.Addr()})
		return err
	})
	assert.Nil(t, err)
	time.Sleep(10 * time.Millisecond)

	assert.Len(t, nodes, 5)
	nodes1, err := nodeman1.Nodes()
	assert.Nil(t, err)
	assert.Len(t, nodes1, 3)
	assert.Nil(t, nodeman1.Shutdown())
	assert.Nil(t, nodeman2.Shutdown())
	assert.Nil(t, nodeman3.Shutdown())
}

func testNodeManager(t *testing.T) *nodeman {
	port, err := getPort()
	assert.Nil(t, err)
	owner := makeNodeInfo("127.0.0.1", port)
	conf := memberlist.DefaultLANConfig()
	conf.LogOutput = bytes.NewBuffer(nil)
	conf.BindPort = port
	nodeManager, err := nodemanFromMemberlistConfig(owner, conf)
	assert.Nil(t, err)
	return nodeManager
}

func tryJoin(fn func() error) (err error) {
	for i := 0; i < 8; i++ {
		time.Sleep(50 * time.Millisecond)
		if err = fn(); err == nil {
			break
		}
	}
	return
}

func getPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
