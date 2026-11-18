package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
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
	// 无在线插件不代表历史上没有外部副本，默认不启用。必须由部署方独立核对后显式确认。
	if os.Getenv("WK_GROUP_PURGE_NO_EXTERNAL_SEARCH") != "verified" || os.Getenv("SEARCH_OPENSEARCH_URL") != "" ||
		service.PluginManager == nil || len(options.G.Plugin.Install) != 0 || len(service.PluginManager.Plugins()) != 0 {
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
	c.JSON(http.StatusOK, map[string]any{"group_no": req.Group, "protocol": 1, "checked": checkOnly, "purged": !checkOnly})
}
