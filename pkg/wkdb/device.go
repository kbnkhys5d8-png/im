package wkdb

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

func (wk *wukongDB) getDeviceId(uid string, deviceFlag uint64) (uint64, error) {
	db := wk.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(uid), 0),
		UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(uid), math.MaxUint64),
	})
	defer iter.Close()

	var selected Device
	for iter.First(); iter.Valid(); iter.Next() {
		_, id, err := key.ParseDeviceSecondIndexKey(iter.Key())
		if err != nil {
			return 0, err
		}
		device, err := wk.getDeviceById(id, db)
		if err != nil {
			return 0, err
		}

		if device.DeviceFlag == deviceFlag && (IsEmptyDevice(selected) || device.TokenVersion > selected.TokenVersion) {
			// 历史库可能有同设备多条记录，优先最高版本，防止重启后选回旧 Token。
			selected = device
		}
	}
	if !IsEmptyDevice(selected) {
		return selected.Id, nil
	}
	return 0, ErrNotFound
}

func (wk *wukongDB) getDeviceById(id uint64, db *pebble.DB) (Device, error) {
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewDeviceColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var device = EmptyDevice
	err := wk.iterDevice(iter, func(d Device) bool {
		device = d
		return false
	})
	if err != nil {
		return EmptyDevice, err
	}
	if IsEmptyDevice(device) {
		return EmptyDevice, ErrNotFound
	}

	return device, nil
}

func (wk *wukongDB) GetDevice(uid string, deviceFlag uint64) (Device, error) {

	wk.metrics.GetDeviceAdd(1)

	return wk.deviceCache.getOrLoadDevice(uid, deviceFlag, func() (Device, error) {
		return wk.getDeviceFromDB(uid, deviceFlag)
	})
}

func (wk *wukongDB) getDeviceFromDB(uid string, deviceFlag uint64) (Device, error) {
	// 缓存未命中，从数据库查询
	// 获取设备id
	id, err := wk.getDeviceId(uid, deviceFlag)
	if err != nil {
		return EmptyDevice, err
	}

	if id == 0 {
		return EmptyDevice, ErrNotFound
	}

	db := wk.shardDB(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewDeviceColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var device = EmptyDevice
	err = wk.iterDevice(iter, func(d Device) bool {
		if d.Id == id {
			device = d
			return false
		}
		return true
	})
	if err != nil {
		if err == pebble.ErrNotFound {
			return EmptyDevice, ErrNotFound
		}
		return EmptyDevice, err
	}

	if device == EmptyDevice {
		return EmptyDevice, ErrNotFound
	}

	return device, nil
}

func (wk *wukongDB) GetDevices(uid string) ([]Device, error) {

	wk.metrics.GetDevicesAdd(1)

	db := wk.shardDB(uid)
	uidHash := key.HashWithString(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, 0),
		UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, math.MaxUint64),
	})
	defer iter.Close()

	var devices []Device
	for iter.First(); iter.Valid(); iter.Next() {
		_, id, err := key.ParseDeviceSecondIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		device, err := wk.getDeviceById(id, db)
		if err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, nil
}

func (wk *wukongDB) GetDeviceCount(uid string) (int, error) {

	wk.metrics.GetDeviceCountAdd(1)

	db := wk.shardDB(uid)
	uidHash := key.HashWithString(uid)
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, 0),
		UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.Column.Uid, uidHash, math.MaxUint64),
	})
	defer iter.Close()

	var count int
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	return count, nil
}

func (wk *wukongDB) AddDevice(d Device) error {

	wk.metrics.AddDeviceAdd(1)

	if d.Id == 0 {
		return ErrInvalidDeviceId
	}
	old, err := wk.GetDevice(d.Uid, d.DeviceFlag)
	if err != nil && err != ErrNotFound {
		return err
	}
	if !IsEmptyDevice(old) && (old.TokenVersion > 0 || d.TokenVersion > 0) {
		if !deviceTokenWriteAllowed(old, d) {
			return nil
		}
		// 旧 Add 日志重放不得创建同一设备的第二条记录。
		d.Id = old.Id
		return wk.UpdateDevice(d)
	}
	db := wk.shardDB(d.Uid)
	batch := db.NewBatch()
	defer batch.Close()
	err = wk.writeDevice(d, batch)
	if err != nil {
		return err
	}
	err = batch.Commit(wk.sync)
	if err != nil {
		return err
	}

	// 更新缓存
	wk.deviceCache.SetDevice(d)

	return nil
}

func (wk *wukongDB) UpdateDevice(d Device) error {
	return wk.updateDeviceWithTokenCondition(d, nil)
}

type deviceTokenCondition struct {
	version uint64
	hash    string
}

func (wk *wukongDB) UpdateDeviceIfTokenMatches(d Device, version uint64, hash string) error {
	return wk.updateDeviceWithTokenCondition(d, &deviceTokenCondition{version: version, hash: hash})
}

func (wk *wukongDB) updateDeviceWithTokenCondition(d Device, expected *deviceTokenCondition) error {

	wk.metrics.UpdateDeviceAdd(1)

	if d.Id == 0 {
		return ErrInvalidDeviceId
	}
	d.CreatedAt = nil // 更新时不更新创建时间

	db := wk.shardDB(d.Uid)
	// 获取旧设备信息
	old, err := wk.getDeviceById(d.Id, db)
	if err != nil && err != ErrNotFound {
		return err
	}
	// 直接按 UID/设备查最高版本，不能让另一条历史重复主键绕过版本保护。
	currentID, lookupErr := wk.getDeviceId(d.Uid, d.DeviceFlag)
	if lookupErr != nil && lookupErr != ErrNotFound {
		return lookupErr
	}
	if currentID != 0 && currentID != d.Id {
		current, readErr := wk.getDeviceById(currentID, db)
		if readErr != nil {
			return readErr
		}
		if current.TokenVersion > old.TokenVersion || (IsEmptyDevice(old) && d.TokenVersion > 0) {
			d.Id, old = current.Id, current
		}
	}
	// 在状态机落库处再次拒绝旧版本；忽略旧日志而不是让 Raft 重试错误。
	if expected != nil {
		hash := sha256.Sum256([]byte(old.Token))
		matches := !IsEmptyDevice(old) && old.TokenVersion == expected.version && hex.EncodeToString(hash[:]) == expected.hash
		alreadyApplied := old.TokenVersion == d.TokenVersion && old.Token == d.Token && old.DeviceLevel == d.DeviceLevel
		if !matches && !alreadyApplied {
			return nil
		}
	}
	if !deviceTokenWriteAllowed(old, d) {
		return nil
	}

	if !IsEmptyDevice(old) {
		old.CreatedAt = nil // 不删除创建时间
		err = wk.deleteDeviceIndex(old, db)
		if err != nil {
			return err
		}
	}

	batch := db.NewBatch()
	defer batch.Close()
	err = wk.writeDevice(d, batch)
	if err != nil {
		return err
	}
	err = batch.Commit(wk.sync)
	if err != nil {
		return err
	}

	// 同一设备直接原子替换，不能先删掉新版本的缓存保护。
	if !IsEmptyDevice(old) {
		if old.Uid != d.Uid || old.DeviceFlag != d.DeviceFlag {
			wk.deviceCache.InvalidateDevice(old.Uid, old.DeviceFlag)
		}
	}
	wk.deviceCache.SetDevice(d)

	return nil
}

func (wk *wukongDB) SearchDevice(req DeviceSearchReq) ([]Device, error) {

	wk.metrics.SearchDeviceAdd(1)

	iterFnc := func(devices *[]Device) func(d Device) bool {
		currentSize := 0
		return func(d Device) bool {
			if req.Pre {
				if req.OffsetCreatedAt > 0 && d.CreatedAt != nil && d.CreatedAt.UnixNano() <= req.OffsetCreatedAt {
					return false
				}
			} else {
				if req.OffsetCreatedAt > 0 && d.CreatedAt != nil && d.CreatedAt.UnixNano() >= req.OffsetCreatedAt {
					return false
				}
			}

			if currentSize > req.Limit {
				return false
			}
			currentSize++
			*devices = append(*devices, d)

			return true
		}
	}

	allDevices := make([]Device, 0, req.Limit*len(wk.dbs))
	indexedDevices := make([]Device, 0, req.Limit)
	has, err := wk.searchDeviceByIndex(req, iterFnc(&indexedDevices))
	if err != nil {
		return nil, err
	}
	if has {
		allDevices = append(allDevices, indexedDevices...)
	} else {
		for _, db := range wk.dbs {
			devices := make([]Device, 0, req.Limit)
			fnc := iterFnc(&devices)

			start := uint64(req.OffsetCreatedAt)
			end := uint64(math.MaxUint64)
			if req.OffsetCreatedAt > 0 {
				if req.Pre {
					start = uint64(req.OffsetCreatedAt + 1)
					end = uint64(math.MaxUint64)
				} else {
					start = 0
					end = uint64(req.OffsetCreatedAt)
				}
			}

			iter := db.NewIter(&pebble.IterOptions{
				LowerBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, start, 0),
				UpperBound: key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, end, 0),
			})
			defer iter.Close()

			var iterStepFnc func() bool
			if req.Pre {
				if !iter.First() {
					continue
				}
				iterStepFnc = iter.Next
			} else {
				if !iter.Last() {
					continue
				}
				iterStepFnc = iter.Prev
			}

			for ; iter.Valid(); iterStepFnc() {
				_, id, err := key.ParseDeviceSecondIndexKey(iter.Key())
				if err != nil {
					return nil, err
				}

				dataIter := db.NewIter(&pebble.IterOptions{
					LowerBound: key.NewDeviceColumnKey(id, key.MinColumnKey),
					UpperBound: key.NewDeviceColumnKey(id, key.MaxColumnKey),
				})
				defer dataIter.Close()

				var d Device
				err = wk.iterDevice(dataIter, func(device Device) bool {
					d = device
					return false
				})
				if err != nil {
					return nil, err
				}
				if !fnc(d) {
					break
				}
			}
			allDevices = append(allDevices, devices...)
		}
	}
	// 降序排序
	sort.Slice(allDevices, func(i, j int) bool {
		return allDevices[i].CreatedAt.UnixNano() > allDevices[j].CreatedAt.UnixNano()
	})

	if req.Limit > 0 && len(allDevices) > req.Limit {
		if req.Pre {
			allDevices = allDevices[len(allDevices)-req.Limit:]
		} else {
			allDevices = allDevices[:req.Limit]
		}
	}

	return allDevices, nil
}

func (wk *wukongDB) searchDeviceByIndex(req DeviceSearchReq, iterFnc func(d Device) bool) (bool, error) {

	if req.Uid != "" && req.DeviceFlag != 0 {
		device, err := wk.GetDevice(req.Uid, req.DeviceFlag)
		if err != nil {
			return false, err
		}
		if !IsEmptyDevice(device) {
			iterFnc(device)
		}
		return true, nil
	}

	if req.Uid != "" {
		devices, err := wk.GetDevices(req.Uid)
		if err != nil {
			return false, err
		}
		for _, d := range devices {
			if !iterFnc(d) {
				return true, nil
			}
		}
		return true, nil
	}

	return false, nil
}

func (wk *wukongDB) writeDevice(d Device, w pebble.Writer) error {
	var (
		err error
	)
	// uid
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.Uid), []byte(d.Uid), wk.noSync); err != nil {
		return err
	}

	// token
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.Token), []byte(d.Token), wk.noSync); err != nil {
		return err
	}
	version := make([]byte, 8)
	wk.endian.PutUint64(version, d.TokenVersion)
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.TokenVersion), version, wk.noSync); err != nil {
		return err
	}

	// deviceFlag
	var deviceFlagBytes = make([]byte, 8)
	wk.endian.PutUint64(deviceFlagBytes, d.DeviceFlag)
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.DeviceFlag), deviceFlagBytes, wk.noSync); err != nil {
		return err
	}

	// deviceLevel
	if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.DeviceLevel), []byte{d.DeviceLevel}, wk.noSync); err != nil {
		return err
	}
	// createdAt
	if d.CreatedAt != nil {
		ct := uint64(d.CreatedAt.UnixNano())
		createdAt := make([]byte, 8)
		wk.endian.PutUint64(createdAt, ct)
		if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.CreatedAt), createdAt, wk.noSync); err != nil {
			return err
		}

		// createdAt second index
		if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, ct, d.Id), nil, wk.noSync); err != nil {
			return err
		}

	}

	if d.UpdatedAt != nil {
		// updatedAt
		updatedAt := make([]byte, 8)
		wk.endian.PutUint64(updatedAt, uint64(d.UpdatedAt.UnixNano()))
		if err = w.Set(key.NewDeviceColumnKey(d.Id, key.TableDevice.Column.UpdatedAt), updatedAt, wk.noSync); err != nil {
			return err
		}

		// updatedAt second index
		if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.UpdatedAt, uint64(d.UpdatedAt.UnixNano()), d.Id), nil, wk.noSync); err != nil {
			return err
		}
	}

	// uid index
	if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(d.Uid), d.Id), nil, wk.noSync); err != nil {
		return err
	}

	// deviceFlag index
	if err = w.Set(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.DeviceFlag, d.DeviceFlag, d.Id), nil, wk.noSync); err != nil {
		return err
	}

	return nil
}

func (wk *wukongDB) deleteDeviceIndex(old Device, w pebble.Writer) error {
	// uid index
	if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.Uid, key.HashWithString(old.Uid), old.Id), wk.noSync); err != nil {
		return err
	}

	// deviceFlag index
	if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.DeviceFlag, old.DeviceFlag, old.Id), wk.noSync); err != nil {
		return err
	}

	// createdAt index
	if old.CreatedAt != nil {
		if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.CreatedAt, uint64(old.CreatedAt.UnixNano()), old.Id), wk.noSync); err != nil {
			return err
		}
	}

	// updatedAt index
	if old.UpdatedAt != nil {
		if err := w.Delete(key.NewDeviceSecondIndexKey(key.TableDevice.SecondIndex.UpdatedAt, uint64(old.UpdatedAt.UnixNano()), old.Id), wk.noSync); err != nil {
			return err
		}
	}

	return nil
}

// 解析出设备信息
// id !=0 时，解析出id对应的用户信息
func (wk *wukongDB) iterDevice(iter *pebble.Iterator, iterFnc func(d Device) bool) error {
	var (
		preId          uint64
		preDevice      Device
		lastNeedAppend bool = true
		hasData        bool = false
	)

	for iter.First(); iter.Valid(); iter.Next() {
		primaryKey, columnName, err := key.ParseDeviceColumnKey(iter.Key())
		if err != nil {
			return err
		}

		if preId != primaryKey {
			if preId != 0 {
				if !iterFnc(preDevice) {
					lastNeedAppend = false
					break
				}
			}
			preId = primaryKey
			preDevice = Device{Id: primaryKey}
		}

		switch columnName {
		case key.TableDevice.Column.Uid:
			preDevice.Uid = string(iter.Value())
		case key.TableDevice.Column.Token:
			preDevice.Token = string(iter.Value())
		case key.TableDevice.Column.TokenVersion:
			if len(iter.Value()) != 8 {
				return fmt.Errorf("invalid device token version length: %d", len(iter.Value()))
			}
			preDevice.TokenVersion = wk.endian.Uint64(iter.Value())
		case key.TableDevice.Column.DeviceFlag:
			preDevice.DeviceFlag = wk.endian.Uint64(iter.Value())
		case key.TableDevice.Column.DeviceLevel:
			preDevice.DeviceLevel = iter.Value()[0]
		case key.TableDevice.Column.CreatedAt:
			tm := int64(wk.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preDevice.CreatedAt = &t
			}

		case key.TableDevice.Column.UpdatedAt:
			tm := int64(wk.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preDevice.UpdatedAt = &t
			}

		}
		lastNeedAppend = true
		hasData = true
	}

	if lastNeedAppend && hasData {
		_ = iterFnc(preDevice)
	}
	return nil
}

func deviceTokenWriteAllowed(old, next Device) bool {
	if old.TokenVersion == 0 && next.TokenVersion == 0 {
		return true
	}
	if next.TokenVersion != old.TokenVersion {
		return next.TokenVersion > old.TokenVersion
	}
	return next.Token == old.Token && next.DeviceLevel == old.DeviceLevel
}
