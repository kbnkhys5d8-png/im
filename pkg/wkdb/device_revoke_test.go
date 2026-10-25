package wkdb_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestDeviceConditionalRevokePreservesLaterLoginAtApply(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir())))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.AddDevice(tokenVersionDevice(t, "old", 41)))
	require.NoError(t, db.UpdateDevice(tokenVersionDevice(t, "new", 43)))
	hash := sha256.Sum256([]byte("old"))
	// 比较条件随日志落库；旧 leader 的更高版本也不能覆盖新会话。
	require.NoError(t, db.UpdateDeviceIfTokenMatches(tokenVersionDevice(t, "revoke", 500), 41, hex.EncodeToString(hash[:])))
	device, err := db.GetDevice("fenced-user", 0)
	require.NoError(t, err)
	require.Equal(t, "new", device.Token)
	require.EqualValues(t, 43, device.TokenVersion)
}

func TestDeviceConditionalRevokeIsReplaySafe(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir())))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.AddDevice(tokenVersionDevice(t, "old", 0)))
	hash := sha256.Sum256([]byte("old"))
	for i := 0; i < 3; i++ {
		require.NoError(t, db.UpdateDeviceIfTokenMatches(tokenVersionDevice(t, "revoke", 42), 0, hex.EncodeToString(hash[:])))
	}
	device, err := db.GetDevice("fenced-user", 0)
	require.NoError(t, err)
	require.Equal(t, "revoke", device.Token)
	require.NoError(t, db.UpdateDevice(tokenVersionDevice(t, "new", 43)))
	require.NoError(t, db.UpdateDeviceIfTokenMatches(tokenVersionDevice(t, "revoke", 42), 0, hex.EncodeToString(hash[:])))
	device, err = db.GetDevice("fenced-user", 0)
	require.NoError(t, err)
	require.Equal(t, "new", device.Token)
}
