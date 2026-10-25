package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestConditionalDeviceCommandKeepsProofAndRedactsDiagnostics(t *testing.T) {
	hash := sha256.Sum256([]byte("old-secret"))
	proof := hex.EncodeToString(hash[:])
	data, err := json.Marshal(conditionalDeviceUpdate{Device: wkdb.Device{Id: 1, Uid: "u", Token: "revoke-secret", TokenVersion: 42, DeviceLevel: 1}, Version: 41, Hash: proof})
	require.NoError(t, err)
	cmd := NewCMD(CMDUpdateDeviceIfTokenMatches, data)
	wire, err := cmd.Marshal()
	require.NoError(t, err)
	var decoded CMD
	require.NoError(t, decoded.Unmarshal(wire))
	device, version, saved, err := decoded.DecodeConditionalDevice()
	require.NoError(t, err)
	require.EqualValues(t, 41, version)
	require.Equal(t, proof, saved)
	require.Equal(t, "revoke-secret", device.Token)
	content, err := decoded.CMDContent()
	require.NoError(t, err)
	require.NotContains(t, content, "revoke-secret")
	require.NotContains(t, content, proof)
	require.NotContains(t, string(slotLogDiagnosticData(wire)), "revoke-secret")
	require.NotContains(t, string(slotLogDiagnosticData(wire)), proof)
	require.Greater(t, int(CMDUpdateDeviceIfTokenMatches), int(CMDUpdateConversationIfSeqGreater))
}

func TestConditionalDeviceCommandRejectsInvalidProofBeforeProposal(t *testing.T) {
	s := New(NewOptions())
	// 没有槽服务；参数校验若晚于提案，这里会直接 panic。
	err := s.UpdateDeviceIfTokenMatches(wkdb.Device{Id: 1, Uid: "u", Token: "revoke", TokenVersion: 42}, 41, "not-a-proof")
	require.Error(t, err)
}
