package store

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestDeviceTokenVersionCommandRoundTripAndLegacy(t *testing.T) {
	for _, version := range []uint64{0, 42} {
		device := wkdb.Device{Id: 1, Uid: "u", Token: "token", TokenVersion: version}
		cmd := NewCMD(CMDUpdateDevice, EncodeCMDDevice(device))
		decoded, err := cmd.DecodeCMDDevice()
		require.NoError(t, err)
		require.Equal(t, device, decoded)
	}
	data := EncodeCMDDevice(wkdb.Device{Id: 1, Uid: "u", Token: "token"})
	_, err := NewCMD(CMDUpdateDevice, append(data, 1)).DecodeCMDDevice()
	require.Error(t, err, "损坏的版本字段不能当成旧格式日志")
}
