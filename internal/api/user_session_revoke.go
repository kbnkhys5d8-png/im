package api

import (
	"context"
	"encoding/hex"
	"net/http"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/pkg/errors"
)

var errDeviceSessionSuperseded = errors.New("原目标会话已变化")

type deviceSessionTarget struct {
	Version uint64 `json:"expected_token_version"`
	Hash    string `json:"expected_token_hash"`
	ctx     context.Context
}

func (target deviceSessionTarget) matches(device wkdb.Device) bool {
	return !wkdb.IsEmptyDevice(device) && device.TokenVersion == target.Version && eventbus.TokenFingerprint(device.Token) == target.Hash
}

func (u *user) deviceSessionSnapshot(c *wkhttp.Context) {
	var req struct {
		UID  string              `json:"uid"`
		Flag *wkproto.DeviceFlag `json:"device_flag"`
	}
	body, err := BindJSON(&req, c)
	if err != nil || req.UID == "" || req.Flag == nil || *req.Flag > wkproto.PC || req.UID == options.G.SystemUID {
		c.ResponseError(errors.New("会话查询参数错误"))
		return
	}
	if u.forwardDeviceSession(c, req.UID, body) {
		return
	}
	var device wkdb.Device
	err = eventbus.User.RunOnUserLeader(c.Request.Context(), req.UID, func() error {
		if err := checkTokenLeader(req.UID); err != nil {
			return err
		}
		var err error
		device, err = service.Store.GetDevice(req.UID, *req.Flag)
		if err != nil && err != wkdb.ErrNotFound {
			return err
		}
		if !wkdb.IsEmptyDevice(device) && (device.Uid != req.UID || device.DeviceFlag != uint64(*req.Flag)) {
			return errors.New("设备会话身份不一致")
		}
		if !wkdb.IsEmptyDevice(device) {
			// 旧内部连接没有认证版本时先拒绝生成证明，避免先撤销凭据后才发现目标不明。
			for _, node := range service.Cluster.Nodes() {
				var conns []*eventbus.Conn
				if options.G.IsLocalNode(node.Id) {
					conns = ingress.LocalUserConns(req.UID)
				} else {
					conns, err = ingress.NewClient().UserConnsContext(c.Request.Context(), node.Id, req.UID)
					if err != nil {
						return err
					}
				}
				for _, conn := range conns {
					if conn.Auth && conn.DeviceFlag == *req.Flag && conn.AuthTokenHash == eventbus.TokenFingerprint(device.Token) && !conn.AuthTokenVersionKnown {
						return errors.New("旧连接缺少认证代际，不能生成退出目标")
					}
				}
			}
		}
		return checkTokenLeader(req.UID)
	})
	if err != nil {
		c.ResponseError(errors.New("查询设备会话失败"))
		return
	}
	exists := !wkdb.IsEmptyDevice(device)
	var version uint64
	var hash string
	if exists {
		version, hash = device.TokenVersion, eventbus.TokenFingerprint(device.Token)
	}
	// 绝不返回原 Token；新 API 必须校验身份、端和完整响应后才保存目标。
	c.JSON(http.StatusOK, map[string]interface{}{"status": http.StatusOK, "uid": req.UID, "device_flag": *req.Flag, "exists": exists, "token_version": version, "token_hash": hash})
}

func (u *user) revokeDeviceSession(c *wkhttp.Context) {
	var req struct {
		UID             string              `json:"uid"`
		Flag            *wkproto.DeviceFlag `json:"device_flag"`
		Token           string              `json:"token"`
		Version         uint64              `json:"token_version"`
		ExpectedVersion *uint64             `json:"expected_token_version"`
		ExpectedHash    string              `json:"expected_token_hash"`
	}
	body, err := BindJSON(&req, c)
	hash, hashErr := hex.DecodeString(req.ExpectedHash)
	if err != nil || req.UID == "" || req.UID == options.G.SystemUID || req.Flag == nil || *req.Flag > wkproto.PC || req.Token == "" || req.Version == 0 || req.Version >= 9007199254740991 || req.ExpectedVersion == nil || req.Version <= *req.ExpectedVersion || hashErr != nil || len(hash) != 32 || eventbus.TokenFingerprint(req.Token) == req.ExpectedHash {
		c.ResponseError(errors.New("条件撤销参数错误"))
		return
	}
	if u.forwardDeviceSession(c, req.UID, body) {
		return
	}
	target := deviceSessionTarget{Version: *req.ExpectedVersion, Hash: req.ExpectedHash, ctx: c.Request.Context()}
	err = eventbus.User.RunOnUserLeader(c.Request.Context(), req.UID, func() error {
		return u.updateTokenOnLeaderExpected(UpdateTokenReq{UID: req.UID, Token: req.Token, TokenVersion: req.Version, DeviceFlag: *req.Flag, DeviceLevel: wkproto.DeviceLevelMaster}, &target)
	})
	result := "revoked"
	if errors.Is(err, errDeviceSessionSuperseded) || errors.Is(err, errTokenVersionConflict) {
		result = "superseded"
	} else if err != nil {
		// 落库后关闭失败也必须返回失败，让同一任务重试完成关闭；不能提前确认。
		c.ResponseError(errors.New("撤销设备会话未完成"))
		return
	}
	c.JSON(http.StatusOK, map[string]interface{}{"status": http.StatusOK, "uid": req.UID, "device_flag": *req.Flag, "token_version": req.Version, "result": result})
}

// 沿用原 Token 接口的 leader 转发，内部路由不能降级到不比较目标的旧接口。
func (u *user) forwardDeviceSession(c *wkhttp.Context, uid string, body []byte) bool {
	leader, err := service.Cluster.SlotLeaderOfChannel(uid, wkproto.ChannelTypePerson)
	if err != nil || leader == nil {
		c.ResponseError(errors.New("获取会话认证节点失败"))
		return true
	}
	if !options.G.IsLocalNode(leader.Id) {
		c.ForwardWithBody(leader.ApiServerAddr+c.Request.URL.Path, body)
		return true
	}
	return false
}
