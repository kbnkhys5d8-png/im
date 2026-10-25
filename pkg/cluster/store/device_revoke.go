package store

import (
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

type conditionalDeviceUpdate struct {
	Device  wkdb.Device `json:"device"`
	Version uint64      `json:"expected_token_version"`
	Hash    string      `json:"expected_token_hash"`
}

// 异常路径同样不能把撤销凭据及原目标指纹写进日志。
func slotLogDiagnosticData(data []byte) []byte {
	var command CMD
	_ = command.Unmarshal(data)
	if command.CmdType == CMDUpdateDeviceIfTokenMatches {
		return []byte("<条件设备撤销载荷已脱敏>")
	}
	return data
}

func (s *Store) UpdateDeviceIfTokenMatches(device wkdb.Device, version uint64, hash string) error {
	data, err := json.Marshal(conditionalDeviceUpdate{Device: device, Version: version, Hash: hash})
	if err != nil {
		return err
	}
	command := NewCMD(CMDUpdateDeviceIfTokenMatches, data)
	// 无效条件必须在提案前失败，不能把不可应用的命令写进槽日志。
	if _, _, _, err := command.DecodeConditionalDevice(); err != nil {
		return err
	}
	cmd, err := command.Marshal()
	if err != nil {
		return err
	}
	_, err = s.opts.Slot.ProposeUntilApplied(s.opts.Slot.GetSlotId(device.Uid), cmd)
	return err
}

func (c *CMD) DecodeConditionalDevice() (wkdb.Device, uint64, string, error) {
	var update conditionalDeviceUpdate
	if err := json.Unmarshal(c.Data, &update); err != nil {
		return wkdb.Device{}, 0, "", err
	}
	hash, err := hex.DecodeString(update.Hash)
	if err != nil || len(hash) != 32 || update.Device.Id == 0 || update.Device.Uid == "" || update.Device.Token == "" || update.Device.TokenVersion <= update.Version || update.Device.TokenVersion >= 9007199254740991 {
		return wkdb.Device{}, 0, "", errors.New("条件设备日志内容异常")
	}
	return update.Device, update.Version, update.Hash, nil
}

func (s *Store) handleConditionalDeviceUpdate(cmd *CMD) error {
	device, version, hash, err := cmd.DecodeConditionalDevice()
	if err != nil {
		return err
	}
	return s.wdb.UpdateDeviceIfTokenMatches(device, version, hash)
}
