package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestClusterNode_FromString(t *testing.T) {
	testCases := []string{
		"127.0.0.1:2000",
		"127.0.0.2:2000",
		"127.0.0.3:2000",
	}
	for _, host := range testCases {
		node := nodeInfoFromAddr(host)
		require.Equal(t, host, node.Addr())
		require.NotZero(t, node.ID())
		require.NotZero(t, node.AddrHash())
		require.NotZero(t, node.Birthdate())
	}
}

func TestClusterNode_EncodeDecode(t *testing.T) {
	testCases := []string{
		"127.0.0.1:2000",
		"127.0.0.2:2000",
		"127.0.0.3:2000",
	}
	for _, host := range testCases {
		node := nodeInfoFromAddr(host)
		data, err := encodeNodeInfo(&node)
		assert.NoError(t, err)
		assert.NotEmpty(t, data)
		node2, err := decodeNodeInfo(data)
		assert.Nil(t, err)
		require.True(t, compareNodeByAddr(node, node2))
		require.True(t, compareNodeByID(node, node2))
		require.True(t, compareNodeByAddrHash(node, node2))
		require.Equal(t, node.Birthdate(), node2.Birthdate())
		require.NotEmpty(t, node2.String())
	}
}
