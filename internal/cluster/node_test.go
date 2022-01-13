package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestNode_FromString(t *testing.T) {
	testCases := []string{
		"127.0.0.1:2000",
		"127.0.0.2:2000",
		"127.0.0.3:2000",
	}
	for _, host := range testCases {
		node := nodeFromString(host)
		require.Equal(t, host, node.Host())
		require.NotZero(t, node.ID())
		require.NotZero(t, node.Hash())
		require.NotZero(t, node.Birthdate())
	}
}

func TestNode_EncodeDecode(t *testing.T) {
	testCases := []string{
		"127.0.0.1:2000",
		"127.0.0.2:2000",
		"127.0.0.3:2000",
	}
	for _, host := range testCases {
		node := nodeFromString(host)
		data, err := encodeNodeToMeta(node)
		assert.NoError(t, err)
		assert.NotEmpty(t, data)
		node2, err := decodeNodeFromMeta(data)
		assert.Nil(t, err)
		require.True(t, compareNodeByHost(node, node2))
		require.True(t, compareNodeByID(node, node2))
		require.True(t, compareNodeByHash(node, node2))
		require.Equal(t, node.Birthdate(), node2.Birthdate())
		require.NotEmpty(t, node2.String())
	}
}
