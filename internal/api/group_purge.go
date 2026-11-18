package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
)

var groupPurgeID = regexp.MustCompile(`^[0-9A-Za-z_-]{1,40}$`)

func (h *historyAPI) groupPurgeCheck(c *wkhttp.Context)   { h.groupPurge(c, true) }
func (h *historyAPI) groupPurgeExecute(c *wkhttp.Context) { h.groupPurge(c, false) }

func (h *historyAPI) groupPurge(c *wkhttp.Context, checkOnly bool) {
	if !historyLocal(c) {
		return
	}
	var req struct {
		Group string `json:"group_no"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 1024))
	dec.DisallowUnknownFields()
	if dec.Decode(&req) != nil || dec.Decode(new(any)) != io.EOF || !groupPurgeID.MatchString(req.Group) {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	search := os.Getenv("WK_GROUP_PURGE_OPENSEARCH") == "verified"
	noSearch := os.Getenv("WK_GROUP_PURGE_NO_EXTERNAL_SEARCH") == "verified"
	// 无在线插件不等于历史没有副本，两种模式均须明确确认且不可同时开启。
	if search == noSearch || service.PluginManager == nil {
		c.AbortWithStatus(http.StatusConflict)
		return
	}
	plugins := service.PluginManager.Plugins()
	var searchPlugin types.Plugin
	if search {
		if options.G.Channel.CmdSuffix != "____cmd" || len(options.G.Plugin.Install) > 1 ||
			len(plugins) != 1 || plugins[0].GetNo() != "wk.plugin.search" ||
			plugins[0].Status() != types.PluginStatusNormal {
			c.AbortWithStatus(http.StatusConflict)
			return
		}
		searchPlugin = plugins[0]
		if err := requestGroupSearchPurge(c.Request.Context(), searchPlugin, req.Group, true); err != nil {
			c.AbortWithStatus(http.StatusConflict)
			return
		}
	} else if os.Getenv("SEARCH_OPENSEARCH_URL") != "" || len(options.G.Plugin.Install) != 0 || len(plugins) != 0 {
		c.AbortWithStatus(http.StatusConflict)
		return
	}
	err := h.store.GroupPurge(c.Request.Context(), req.Group, options.G.OrginalConvertCmdChannel(req.Group), checkOnly)
	if errors.Is(err, wkdb.ErrHistoryUnsupported) || errors.Is(err, wkdb.ErrGroupMessagePurgeUnsupported) {
		c.AbortWithStatus(http.StatusConflict)
		return
	}
	if err != nil {
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	if search && !checkOnly {
		if err := requestGroupSearchPurge(c.Request.Context(), searchPlugin, req.Group, false); err != nil {
			// IM 已清理。索引尚未核实，保留原任务，不能用预检拒绝回执撤销预约。
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
	}
	c.JSON(http.StatusOK, map[string]any{"group_no": req.Group, "protocol": 1, "checked": checkOnly, "purged": !checkOnly,
		"search_enabled": search, "search_verified": search && !checkOnly})
}

func requestGroupSearchPurge(ctx context.Context, plugin types.Plugin, id string, check bool) error {
	path := "/internal/group-purge"
	if check {
		path += "-check"
	}
	body, err := json.Marshal(map[string]string{"group_no": id})
	if err != nil {
		return err
	}
	reply, err := plugin.Route(ctx, &pluginproto.HttpRequest{Path: path, Method: http.MethodPost, Body: body})
	if err != nil || reply == nil || reply.Status != http.StatusOK || len(reply.Body) > 1024 {
		return errors.New("group search purge was not confirmed")
	}
	var proof struct {
		ID       string `json:"group_no"`
		Protocol int    `json:"protocol"`
		Checked  *bool  `json:"checked"`
		Empty    *bool  `json:"verified_empty"`
	}
	dec := json.NewDecoder(bytes.NewReader(reply.Body))
	dec.DisallowUnknownFields()
	if dec.Decode(&proof) != nil || dec.Decode(new(any)) != io.EOF || proof.ID != id || proof.Protocol != 1 ||
		proof.Checked == nil || *proof.Checked != check || proof.Empty == nil || *proof.Empty == check {
		return errors.New("invalid group search purge receipt")
	}
	return nil
}
