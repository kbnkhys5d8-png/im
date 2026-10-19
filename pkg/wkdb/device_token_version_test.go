package wkdb_test

import (
	"encoding/json"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func tokenVersionDevice(t *testing.T, token string, version uint64) wkdb.Device {
	t.Helper()
	data, err := json.Marshal(map[string]interface{}{"id": 1, "uid": "fenced-user", "device_flag": 0, "device_level": 1, "token": token, "token_version": version})
	require.NoError(t, err)
	var d wkdb.Device
	require.NoError(t, json.Unmarshal(data, &d))
	return d
}

func TestDeviceTokenVersionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.AddDevice(tokenVersionDevice(t, "new-token", 42)))
	require.NoError(t, db.Close())
	db = wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir)))
	require.NoError(t, db.Open())
	d, err := db.GetDevice("fenced-user", 0)
	require.NoError(t, err)
	data, err := json.Marshal(d)
	require.NoError(t, err)
	var saved map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &saved))
	require.Equal(t, float64(42), saved["token_version"])
}

func TestDeviceTokenVersionRejectsDelayedAndLegacyWrites(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir())))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.AddDevice(tokenVersionDevice(t, "new-token", 42)))
	for _, version := range []uint64{41, 0, 42} {
		require.NoError(t, db.UpdateDevice(tokenVersionDevice(t, "late-revoke", version)))
		d, err := db.GetDevice("fenced-user", 0)
		require.NoError(t, err)
		require.Equal(t, "new-token", d.Token, "旧版本和同版本不同内容均不能覆盖新 Token")
	}
	// 更早创建的 AddDevice 日志晚到时，同样不能插入第二条设备记录。
	late := tokenVersionDevice(t, "late-add", 40)
	late.Id = 2
	require.NoError(t, db.AddDevice(late))
	require.NoError(t, db.UpdateDevice(late))
	devices, err := db.GetDevices("fenced-user")
	require.NoError(t, err)
	require.Len(t, devices, 1)
	require.Equal(t, "new-token", devices[0].Token)
}

func TestDeviceTokenVersionProtectsLegacyDuplicateDeviceIDs(t *testing.T) {
	dir := t.TempDir()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	first := tokenVersionDevice(t, "legacy-one", 0)
	second := tokenVersionDevice(t, "legacy-two", 0)
	second.Id = 2
	require.NoError(t, db.AddDevice(first))
	require.NoError(t, db.AddDevice(second))
	require.NoError(t, db.UpdateDevice(tokenVersionDevice(t, "current", 42)))
	second.Token, second.TokenVersion = "late-old-id", 41
	require.NoError(t, db.UpdateDevice(second))
	d, err := db.GetDevice("fenced-user", 0)
	require.NoError(t, err)
	require.Equal(t, "current", d.Token)
	require.NoError(t, db.Close())
	db = wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir)))
	require.NoError(t, db.Open())
	d, err = db.GetDevice("fenced-user", 0)
	require.NoError(t, err)
	require.Equal(t, "current", d.Token, "重启后也必须选择最高版本，不能回到历史重复记录")
}
