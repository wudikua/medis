package admin

import (
	// "encoding/json"
	"github.com/julienschmidt/httprouter"
	"io"
	"medis/datasource"
	"medis/logger"
	"medis/mysql"
	"medis/shard"
	"net/http"
	"os"
	"strconv"
)

type AdminServer struct {
	tplDir   string
	selector *shard.Selector
	groups   map[string]*datasource.Group
}

var Server *AdminServer

func (self *AdminServer) Groups(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Write([]byte("hello world"))
}

func (self *AdminServer) AddDs2Group(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	name := r.FormValue("name")
	pwd := r.FormValue("pwd")
	host := r.FormValue("host")
	port, _ := strconv.Atoi(r.FormValue("port"))
	db := r.FormValue("db")
	charset := r.FormValue("charset")
	ds, err := mysql.NewMysqlClient(name, pwd, host, port, db, charset)
	if err != nil {
		self.DisplayError(err, w)
		return
	}
	dsw, _ := strconv.Atoi(r.FormValue("w"))
	dsp, _ := strconv.Atoi(r.FormValue("p"))
	dsr, _ := strconv.Atoi(r.FormValue("r"))
	dsq, _ := strconv.Atoi(r.FormValue("q"))
	dsName := r.FormValue("ds_name")
	wapper := datasource.NewClientWeightWrapper(dsName, ds, dsw, dsp, dsr, dsq)
	groupName := r.FormValue("group_name")
	if self.groups[groupName] == nil {
		w.Write([]byte("new group " + groupName + " first"))
		return
	} else {
		group := self.groups[groupName]
		group.AddClient(wapper)
	}
	logger.LogDebug("group add ", name, pwd, host, port, db, charset)
	w.Write([]byte("success"))
}

func (self *AdminServer) NewGroup(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	groupName := r.FormValue("group_name")
	group := datasource.NewGroup(groupName)
	self.groups[groupName] = group
	self.selector.AddGroup(group)
	w.Write([]byte("success"))
}

func (self *AdminServer) Balance(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	groupName := r.FormValue("group_name")
	group := self.groups[groupName]
	group.Init()
	self.selector.AddScaleGroup(group)
	self.selector.Balance()
	logger.LogDebug("rebalance")
	w.Write([]byte("success"))
}

func (self *AdminServer) EndBalance(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	self.selector.EndBalance()
	w.Write([]byte("success"))
}

func (self *AdminServer) DisplayError(err error, w io.Writer) {
	w.Write([]byte(err.Error()))
}

func RegisterSelector(selector *shard.Selector) {
	Server.selector = selector
}

// 管理后台
func NewAdminServer(addr string) {
	router := httprouter.New()
	Server = &AdminServer{}
	Server.groups = make(map[string]*datasource.Group)
	Server.tplDir, _ = os.Getwd()
	Server.tplDir += "/admin/tpl/"
	router.GET("/group/list", Server.Groups)
	router.GET("/group/add", Server.AddDs2Group)
	router.GET("/group/new", Server.NewGroup)
	router.GET("/group/balance", Server.Balance)
	router.GET("/group/endbalance", Server.EndBalance)
	go http.ListenAndServe(addr, router)
}
