package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

var historyTaskID = regexp.MustCompile(`^[0-9a-f]{64}$`)

type historyRequest struct {
	TaskID    string  `json:"task_id"`
	SourceUID string  `json:"source_uid"`
	PeerUID   string  `json:"peer_uid"`
	AfterSeq  uint64  `json:"after_seq"`
	Limit     int     `json:"limit"`
	High      *uint64 `json:"high_watermark,omitempty"`
}

func (r historyRequest) identity() (wkdb.HistoryFence, error) {
	if !historyTaskID.MatchString(r.TaskID) || r.SourceUID == "" || r.PeerUID == "" || r.SourceUID == r.PeerUID ||
		!utf8.ValidString(r.SourceUID+r.PeerUID) || strings.TrimSpace(r.SourceUID) != r.SourceUID || strings.TrimSpace(r.PeerUID) != r.PeerUID ||
		strings.Contains(r.SourceUID+r.PeerUID, "@") || wkutil.HashCrc32(r.SourceUID) == wkutil.HashCrc32(r.PeerUID) {
		return wkdb.HistoryFence{}, errors.New("invalid history identity")
	}
	return wkdb.HistoryFence{TaskID: r.TaskID, SourceUID: r.SourceUID, PeerUID: r.PeerUID, ChannelID: options.GetFakeChannelIDWith(r.SourceUID, r.PeerUID)}, nil
}

type historyAPI struct{ store *store.Store }

func (h *historyAPI) route(r *wkhttp.WKHttp) {
	r.POST("/history/capabilities", h.capabilities)
	r.POST("/history/fence", h.freeze)
	r.POST("/history/export", h.export)
	r.POST("/history/preflight", h.preflight)
	r.POST("/history/fence/release", h.release)
}

func (h *historyAPI) capabilities(c *wkhttp.Context) {
	if !historyLocal(c) {
		return
	}
	if err := h.store.HistoryCapabilities(); err != nil {
		c.JSON(http.StatusNotImplemented, map[string]string{"error": "single node history is not ready"})
		return
	}
	c.JSON(http.StatusOK, map[string]any{"single_node": true, "protocol": 1})
}

func historyLocal(c *wkhttp.Context) bool {
	host, _, err := net.SplitHostPort(c.Request.RemoteAddr)
	if err != nil || !net.ParseIP(host).IsLoopback() || options.G.ManagerToken == "" ||
		subtle.ConstantTimeCompare([]byte(c.GetHeader("token")), []byte(options.G.ManagerToken)) != 1 {
		c.AbortWithStatus(http.StatusForbidden)
		return false
	}
	return true
}

func historyInput(c *wkhttp.Context, preview bool) (historyRequest, wkdb.HistoryFence, bool) {
	var req historyRequest
	// 只信真实 TCP 对端，X-Forwarded-For 等代理头不能授予 loopback 权限。
	if !historyLocal(c) {
		return req, wkdb.HistoryFence{}, false
	}
	var err error
	decoder := json.NewDecoder(io.LimitReader(c.Request.Body, 4097))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&req); err != nil || (!preview && req.High != nil) {
		c.ResponseError(errors.New("invalid history request"))
		return req, wkdb.HistoryFence{}, false
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		c.ResponseError(errors.New("invalid history request"))
		return req, wkdb.HistoryFence{}, false
	}
	identity, err := req.identity()
	if err != nil {
		c.ResponseError(err)
		return req, identity, false
	}
	return req, identity, true
}

func (h *historyAPI) freeze(c *wkhttp.Context) {
	_, identity, ok := historyInput(c, false)
	if !ok {
		return
	}
	fence, err := h.store.FreezeHistory(c.Request.Context(), identity)
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, fence)
}

func (h *historyAPI) release(c *wkhttp.Context) {
	_, identity, ok := historyInput(c, false)
	if !ok {
		return
	}
	if err := h.store.ReleaseHistory(c.Request.Context(), identity); err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, map[string]bool{"released": true})
}

type historyResponse struct {
	SourceChannel string               `json:"source_channel"`
	Messages      []*types.MessageResp `json:"messages"`
	NextSeq       uint64               `json:"next_seq"`
	More          bool                 `json:"more"`
	HighWatermark uint64               `json:"high_watermark"`
}

func (h *historyAPI) export(c *wkhttp.Context) {
	h.readHistory(c, false)
}

func (h *historyAPI) preflight(c *wkhttp.Context) {
	h.readHistory(c, true)
}

func (h *historyAPI) readHistory(c *wkhttp.Context, preview bool) {
	req, identity, ok := historyInput(c, preview)
	if !ok {
		return
	}
	var page store.HistoryExportPage
	var err error
	if preview {
		page, err = h.store.PreviewHistory(c.Request.Context(), identity, req.AfterSeq, req.Limit, req.High)
	} else {
		page, err = h.store.ExportHistory(c.Request.Context(), identity, req.AfterSeq, req.Limit)
	}
	if err != nil {
		if errors.Is(err, store.ErrHistoryMessageTooLarge) {
			historyMessageTooLarge(c)
			return
		}
		if errors.Is(err, wkdb.ErrHistoryStreamUnsupported) {
			c.JSON(http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
			return
		}
		c.ResponseError(err)
		return
	}
	response := historyResponse{SourceChannel: identity.ChannelID, Messages: []*types.MessageResp{}, NextSeq: req.AfterSeq, More: page.More, HighWatermark: page.HighWatermark}
	for _, message := range page.Messages {
		if message.StreamNo != "" || message.Setting.IsSet(wkproto.SettingStream) {
			c.JSON(http.StatusUnprocessableEntity, map[string]string{"error": "stream history is unsupported"})
			return
		}
		out := &types.MessageResp{}
		out.From(message, options.G.SystemUID)
		// 导出必须保留实际存储频道，不能沿用普通私聊展示的 logical UID。
		out.ChannelID = message.ChannelID
		out.FromUID = message.FromUID
		response.Messages = append(response.Messages, out)
		response.NextSeq = uint64(message.MessageSeq)
		encoded, err := json.Marshal(response)
		if err != nil {
			c.ResponseError(err)
			return
		}
		// 预留终页归一到水位时的数字与布尔字段长度差。
		if len(encoded) > store.HistoryPageBytes-32 {
			response.Messages = response.Messages[:len(response.Messages)-1]
			if len(response.Messages) == 0 {
				historyMessageTooLarge(c)
				return
			}
			response.NextSeq = response.Messages[len(response.Messages)-1].MessageSeq
			response.More = true
			break
		}
	}
	if !response.More {
		response.NextSeq = page.HighWatermark
	}
	// 成功响应保证游标前进，终页允许空数组，但不制造可无限补拉的 more=true。
	if response.More && response.NextSeq <= req.AfterSeq {
		c.ResponseError(errors.New("history cursor did not advance"))
		return
	}
	c.JSON(http.StatusOK, response)
}

// 两种单条超限都不可重试，只返回固定类别，不返回原文或游标。
func historyMessageTooLarge(c *wkhttp.Context) {
	c.JSON(http.StatusUnprocessableEntity, map[string]string{"error_code": "history_message_too_large"})
}

func newHistoryAPI() *historyAPI { return &historyAPI{store: service.Store} }
