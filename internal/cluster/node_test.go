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
		node := NodeFromString(host)
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
		node := NodeFromString(host)
		data, err := EncodeNodeToMeta(node)
		assert.NoError(t, err)
		assert.NotEmpty(t, data)
		node2, err := DecodeNodeFromMeta(data)
		assert.Nil(t, err)
		require.True(t, CompareNodeByHost(node, node2))
		require.True(t, CompareNodeByID(node, node2))
		require.True(t, CompareNodeByHash(node, node2))
		require.Equal(t, node.Birthdate(), node2.Birthdate())
		require.NotEmpty(t, node2.String())
	}
}
