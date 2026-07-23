package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/version"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type DemoServer struct {
	r          *wkhttp.WKHttp
	addr       string
	s          *Server
	httpServer *http.Server
	wklog.Log
}

// NewDemoServer new一个demo server
func NewDemoServer(s *Server) *DemoServer {
	// r := wkhttp.New()
	log := wklog.NewWKLog("DemoServer")
	r := wkhttp.NewWithLogger(wkhttp.LoggerWithWklog(log))
	r.Use(wkhttp.CORSMiddleware())

	ds := &DemoServer{
		r:    r,
		addr: s.opts.Demo.Addr,
		s:    s,
		Log:  log,
	}
	return ds
}

// Start 开始
func (s *DemoServer) Start() {

	s.r.GetGinRoute().Use(gzip.Gzip(gzip.DefaultCompression))

	st, _ := fs.Sub(version.DemoFs, "demo/chatdemo/dist")
	s.r.GetGinRoute().NoRoute(func(c *gin.Context) {

		if c.Request.URL.Path == "" || c.Request.URL.Path == "/" {
			c.Redirect(http.StatusFound, fmt.Sprintf("/chatdemo?apiurl=%s", s.s.opts.External.APIUrl))
			c.Abort()
			return
		}

		if strings.HasPrefix(c.Request.URL.Path, "/chatdemo") {
			c.FileFromFS("./", http.FS(st))
			return
		}
	})

	s.r.GetGinRoute().StaticFS("/chatdemo", http.FS(st))

	s.setRoutes()
	s.httpServer = &http.Server{
		Addr:    s.addr,
		Handler: s.r,
	}
	go func(server *http.Server) {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}(s.httpServer)
	s.Info("Demo server started", zap.String("addr", s.addr))

	_, port := parseAddr(s.addr)
	s.Info(fmt.Sprintf("Chat demo address： http://localhost:%d/chatdemo", port))
}

// Stop 停止服务
func (s *DemoServer) Stop() {
	if s.httpServer == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.Warn("Demo server shutdown failed", zap.Error(err))
	}
}

func (s *DemoServer) setRoutes() {

}
