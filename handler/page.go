/*
 * This file is part of Finn.
 *
 * Copyright (c) 2020 Jan de Visser <jan@finiandarcy.com>
 *
 * Finn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Finn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Finn.  If not, see <https://www.gnu.org/licenses/>.
 */

package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/JanDeVisser/grumble"
	"github.com/JanDeVisser/grumble/tools"
)

type Mount struct {
	Pattern string
	Handler string
	Static  bool
	Config  map[string]interface{}
}

type PipelineEntry struct {
	Handler string
	Config  map[string]interface{}
}

type About struct {
	Copyright  string
	Contact    string
	ContactURL string
	AppName    string `json:"application_name"`
}

type AppConfig struct {
	Mounts         []Mount
	Pipeline       []PipelineEntry
	Icon           string
	Author         string
	Version        string
	AppId          string `json:"app_id"`
	About          *About
	MaxAge         int `json:"max_age"`
	maxAgeDuration time.Duration
}

type Session struct {
	Id      string
	Expires time.Time
	data    map[string]interface{}
}

var sessions = make(map[string]*Session, 0)

func MakeSession(w http.ResponseWriter, req *http.Request) (ret *Session) {
	if cookie, err := req.Cookie("grumbleid"); err == nil {
		ret = sessions[cookie.Value]
		if config.maxAgeDuration > 0 {
			if time.Now().After(ret.Expires) {
				ret = nil
			} else {
				ret.Expires = time.Now().Add(config.maxAgeDuration)
			}
		}
	}
	if ret == nil {
		ret = &Session{}
		ret.Id = uuid.New().String()
		if config.maxAgeDuration > 0 {
			ret.Expires = time.Now().Add(config.maxAgeDuration)
		}
		ret.data = make(map[string]interface{})
		sessions[ret.Id] = ret
		http.SetCookie(w, &http.Cookie{
			Name:   "grumbleid",
			Value:  ret.Id,
			Path:   "/",
			MaxAge: config.MaxAge,
		})
	}
	return
}

func (session *Session) Get(key string) (value interface{}, ok bool) {
	value, ok = session.data[key]
	return
}

func (session *Session) Set(key string, value interface{}) {
	session.data[key] = value
}

func (session *Session) Has(key string) (ret bool) {
	_, ret = session.data[key]
	return
}

/* ----------------------------------------------------------------------- */

var HandlerFncs = make(map[string]func(http.ResponseWriter, *http.Request))
var HandlerObjs = make(map[string]func(*Mount) (http.Handler, error))
var config = AppConfig{}

func serveFileHandler(w http.ResponseWriter, r *http.Request) {
	serveFile(w, r, r.URL.Path)
}

func serveFile(w http.ResponseWriter, r *http.Request, file string) {
	contentTypes := map[string]string{
		"css":  "text/css",
		"html": "text/html",
		"txt":  "text/plain",
		"ico":  "image/jpeg",
		"js":   "text/javascript",
		"jpg":  "image/jpeg",
		"png":  "image/png",
		"gif":  "image/gif",
	}

	ext := file[strings.LastIndex(file, ".")+1:]
	ct, ok := contentTypes[ext]
	if !ok {
		ct = "text/plain"
	}
	w.Header().Add("Content-type", ct)
	if strings.Index(file, "/") == 0 {
		file = file[1:]
	}
	log.Printf("Static File %s (%s)", file, ct)
	http.ServeFile(w, r, file)
}

func AppIcon(w http.ResponseWriter, r *http.Request) {
	if config.Icon == "" {
		config.Icon = "/favicon.ico"
	}
	serveFile(w, r, config.Icon)
}

// --------------------------------------------------------------------------

type StaticFileHandler struct {
	DirPrefix string
}

func MakeStaticHandler(mount *Mount) (ret http.Handler, err error) {
	ret = &StaticFileHandler{}
	sfh := ret.(*StaticFileHandler)
	sfh.DirPrefix = "html/"
	if pref, ok := mount.Config["Prefix"]; ok {
		if prefix, ok := pref.(string); ok {
			sfh.DirPrefix = prefix
		}
	}
	return
}

func (sfh *StaticFileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	serveFile(w, r, sfh.DirPrefix+r.URL.Path)
}

// --------------------------------------------------------------------------

type RedirectHandler struct {
	Redirect string
}

func MakeRedirectHandler(mount *Mount) (ret http.Handler, err error) {
	ret = &RedirectHandler{}
	rh := ret.(*RedirectHandler)
	if redir, ok := mount.Config["Redirect"]; ok {
		if redirect, ok := redir.(string); ok {
			rh.Redirect = redirect
		}
	}
	return
}

func (rh *RedirectHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, rh.Redirect, http.StatusFound)
}

//func HTMLFile(w http.ResponseWriter, r *http.Request) {
//	serveFile(w, r, "/html"+r.URL.Path)
//}

// --------------------------------------------------------------------------

type ContextMaker interface {
	MakeContext(req *PlainRequest) (err error)
}

type PlainRequest struct {
	Manager  *grumble.EntityManager
	Method   string
	Template string
	Data     interface{}
	CtxMaker ContextMaker
	Response http.ResponseWriter
	Request  *http.Request
}

func NewPlainRequest(mgr *grumble.EntityManager, w http.ResponseWriter, r *http.Request, ctxMaker ContextMaker) (ret *PlainRequest, err error) {
	ret = &PlainRequest{Manager: mgr, Response: w, Request: r, CtxMaker: ctxMaker}
	ret.Method = r.Method
	//s := strings.Split(r.URL.Path[1:], "/")
	return
}

func (req *PlainRequest) Execute() {
	v := reflect.ValueOf(req)
	m := v.MethodByName(req.Method)
	if m.IsValid() {
		m.Call([]reflect.Value{})
	} else {
		panic(fmt.Sprintf("Cannot serve method %q for entity requests", req.Method))
	}
}

func (req *PlainRequest) GET() {
	if req.CtxMaker != nil {
		if err := req.CtxMaker.MakeContext(req); err != nil {
			http.Error(req.Response, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		req.Data = nil
	}
	if req.Template == "" {
		req.Template = "html/" + req.Request.RequestURI
	}
	req.ServeTemplate()
}

func (req *PlainRequest) POST() {
	req.GET()
}

func (req *PlainRequest) ServeTemplate() {
	templateManager.Serve(req.Template, req.Data, RootTmpl, req.Response, req.Request)
}

// --------------------------------------------------------------------------

func ServeTemplate(name string, data interface{}, w http.ResponseWriter, r *http.Request) {
	templateManager.Serve(name, data, RootTmpl, w, r)
}

const RootTmpl = "html/template.html"
const EntityTmpl = "html/entity.html"

type TemplateFile struct {
	Mgr       *TemplateManager
	Name      string
	Parent    *TemplateFile
	Template  *template.Template
	Timestamp time.Time
	Functions template.FuncMap
}

func MakeTemplateFile(mgr *TemplateManager, name string, parent *TemplateFile) (tmpl *TemplateFile) {
	tmpl = &TemplateFile{Mgr: mgr, Name: name, Parent: parent}
	return
}

func (tmpl *TemplateFile) UpToDate() (ret bool) {
	if fi, err := os.Stat(tmpl.Name); err != nil {
		panic(fmt.Sprintf("Could not access template file %q", tmpl.Name))
	} else {
		ret = tmpl.Timestamp.Equal(fi.ModTime()) || tmpl.Timestamp.After(fi.ModTime())
		if !ret {
			log.Printf("Template file %q of template %q is outdated", fi.Name(), tmpl.Name)
		}
		if tmpl.Parent != nil {
			ret = ret && tmpl.Parent.UpToDate()
		}
		return
	}
}

func (tmpl *TemplateFile) GetTemplate() (ret *template.Template) {
	fi, err := os.Stat(tmpl.Name)
	if err != nil {
		panic(fmt.Sprintf("Could not access template file %q", tmpl.Name))
	} else {
		if tmpl.Template != nil {
			if !tmpl.UpToDate() {
				log.Printf("Invalidating template %q", tmpl.Name)
				tmpl.Template = nil
			}
		}
	}
	if ret = tmpl.Template; ret == nil {
		if tmpl.Parent != nil {
			parentTmpl := tmpl.Parent.GetTemplate()
			log.Printf("Parsing template %q with parent %q", tmpl.Name, tmpl.Parent.Name)
			tmpl.Template = template.Must(parentTmpl.Clone())
			tmpl.Template = template.Must(tmpl.Template.ParseFiles(tmpl.Name))
		} else {
			log.Printf("Parsing template %q", tmpl.Name)
			tmpl.Template = template.Must(template.ParseFiles(tmpl.Name))
		}
		if tmpl.Functions != nil {
			tmpl.Template.Funcs(tmpl.Functions)
		}
		tmpl.Timestamp = fi.ModTime()
		ret = tmpl.Template
	}
	return
}

type TemplateManager struct {
	Templates map[string]*TemplateFile
}

var templateManager *TemplateManager

func MakeTemplateManager() (ret *TemplateManager) {
	ret = &TemplateManager{}
	ret.Reset()
	return
}

func (mgr *TemplateManager) Reset() {
	mgr.Templates = make(map[string]*TemplateFile)
	rootTemplate := mgr.InitOrGet(RootTmpl, nil)
	rootTemplate.Functions = templateFunctions
	_ = mgr.InitOrGet(EntityTmpl, rootTemplate)
}

func (mgr *TemplateManager) Get(name string) (tmpl *TemplateFile) {
	return mgr.Templates[name]
}

func (mgr *TemplateManager) InitOrGet(name string, parent *TemplateFile) (tmpl *TemplateFile) {
	if tmpl = mgr.Get(name); tmpl == nil {
		tmpl = MakeTemplateFile(mgr, name, parent)
		mgr.Templates[name] = tmpl
	}
	return
}

const ExecKey = "__EXECKEY"

var Executing = make(map[string]*template.Template, 0)

func (mgr *TemplateManager) Serve(name string, data interface{}, parent string, w http.ResponseWriter, r *http.Request) {
	var p *TemplateFile
	if parent != "" {
		p = mgr.Get(parent)
		if p == nil {
			panic(fmt.Sprintf("Undefined parent template %q for template %q", parent, name))
		}
	}

	templateFile := mgr.InitOrGet(name, p)
	textTemplate := templateFile.GetTemplate()
	key := ""
	if ctx, ok := data.(map[string]interface{}); ok {
		key = uuid.New().String()
		ctx[ExecKey] = key
		Executing[key] = textTemplate
		defer delete(Executing, key)
	}
	err := textTemplate.Execute(w, data)
	if err != nil {
		log.Printf("Error Executing template: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ServePlainPage(w http.ResponseWriter, r *http.Request, ctxMaker ContextMaker) {
	mgr, err := grumble.MakeEntityManager()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req, err := NewPlainRequest(mgr, w, r, ctxMaker)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		req.Execute()
	}
}

func PlainPage(w http.ResponseWriter, r *http.Request) {
	ServePlainPage(w, r, nil)
}

func RegisterPipelineFnc(name string, h PipelineService) {
	PipelineHandlers[name] = h
}

func RegisterHandlerFnc(name string, fnc func(http.ResponseWriter, *http.Request)) {
	HandlerFncs[name] = fnc
}

func RegisterHandlerObj(name string, h func(*Mount) (http.Handler, error)) {
	HandlerObjs[name] = h
}

type HandlerWrapper struct {
	mount *Mount
	fnc   func(http.ResponseWriter, *http.Request)
	obj   http.Handler
}

type PipelineService interface {
	SetConfig(config map[string]interface{})
	Entry(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) *http.Request
	Exit(h *HandlerWrapper, w http.ResponseWriter, req *http.Request)
}

var PipelineHandlers = make(map[string]PipelineService)
var Pipeline = make([]PipelineService, 0)

func RegisterPipelineService(svc string, handler PipelineService) {
	PipelineHandlers[svc] = handler
}

func InstallPipelineEntry(svc string, config map[string]interface{}) {
	if h, ok := PipelineHandlers[svc]; !ok {
		log.Fatalf("Unknown pipeline handler %q", svc)
	} else {
		h.SetConfig(config)
		Pipeline = append(Pipeline, h)
	}
}

type ContextKey string

var sessionKey = ContextKey("__SessionKey")
var startTimeKey = ContextKey("__StartTime")

type DummyPipelineEntry struct {
	Config map[string]interface{}
}

func (dummy *DummyPipelineEntry) SetConfig(config map[string]interface{}) {
	dummy.Config = config
}

func (dummy *DummyPipelineEntry) Entry(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) *http.Request {
	return req
}

func (dummy *DummyPipelineEntry) Exit(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) {
}

type AttachSession struct {
	DummyPipelineEntry
}

func (as *AttachSession) Entry(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) *http.Request {
	session := MakeSession(w, req)
	return req.WithContext(context.WithValue(req.Context(), sessionKey, session))
}

type Authenticate struct {
	DummyPipelineEntry
}

func (auth *Authenticate) Entry(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) *http.Request {
	return req
}

type LogRequest struct {
	DummyPipelineEntry
}

func (lr *LogRequest) Entry(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) *http.Request {
	log.Printf("%s %s?%s [%s]", req.Method, req.URL.Path, req.URL.RawQuery, h.mount.Handler)
	return req.WithContext(context.WithValue(req.Context(), startTimeKey, time.Now()))
}

func (lr *LogRequest) Exit(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) {
	startTime := req.Context().Value(startTimeKey).(time.Time)
	d := time.Now().Sub(startTime)
	log.Printf("%s [%dms]", req.RequestURI, d.Milliseconds())
}

type ServeRequest struct {
	DummyPipelineEntry
}

func (sr *ServeRequest) Entry(h *HandlerWrapper, w http.ResponseWriter, req *http.Request) *http.Request {
	if h.obj != nil {
		h.obj.ServeHTTP(w, req)
	} else {
		h.fnc(w, req)
	}
	return req
}

func (wrapper *HandlerWrapper) pipelineEntry(w http.ResponseWriter, req *http.Request, pipeline []PipelineService) {
	if pipeline == nil || len(pipeline) == 0 {
		return
	}
	current := pipeline[0]
	if req = current.Entry(wrapper, w, req); req != nil {
		wrapper.pipelineEntry(w, req, pipeline[1:])
		current.Exit(wrapper, w, req)
	}
}

func (wrapper *HandlerWrapper) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	wrapper.pipelineEntry(w, req, Pipeline)
}

func GetSession(req *http.Request) *Session {
	return req.Context().Value(sessionKey).(*Session)
}

func (mount *Mount) Install() {
	if mount.Static {
		http.HandleFunc(mount.Pattern, serveFileHandler)
	} else {
		wrapper := &HandlerWrapper{mount: mount}
		var ok bool
		if wrapper.fnc, ok = HandlerFncs[mount.Handler]; !ok {
			if factory, ok := HandlerObjs[mount.Handler]; !ok {
				log.Fatalf("Handler '%s' not registered", mount.Handler)
			} else {
				var err error
				if wrapper.obj, err = factory(mount); err != nil {
					log.Fatalf("Could not create handler object for handler %q: %v", mount.Handler, err)
				}
			}
		}
		http.Handle(mount.Pattern, wrapper)
	}
}

func AddMount(pattern string, h string) {
	m := Mount{
		Pattern: pattern,
		Handler: h,
		Static:  false,
	}
	config.Mounts = append(config.Mounts, m)
	m.Install()
}

func AddStatic(pattern string, h string) {
	m := Mount{
		Pattern: pattern,
		Handler: "",
		Static:  true,
	}
	config.Mounts = append(config.Mounts, m)
	m.Install()
}

func StartApp(sync bool) {
	var jsonText []byte
	var err error
	if jsonText, err = ioutil.ReadFile("conf/app.json"); err == nil {
		err = json.Unmarshal(jsonText, &config)
		if err != nil {
			panic(fmt.Sprintf("Could not JSON decode app config: %s", err))
		}
		appicon := false
		for _, mount := range config.Mounts {
			appicon = appicon || (mount.Pattern == "/favicon.ico")
			mount.Install()
		}
		if !appicon {
			AddMount("/favicon.ico", "AppIcon")
		}
		for _, ple := range config.Pipeline {
			InstallPipelineEntry(ple.Handler, ple.Config)
		}
	} else {
		panic(fmt.Sprintf("Could not read app config: %s", err))
	}
	if config.MaxAge > 0 {
		config.maxAgeDuration, _ = time.ParseDuration(fmt.Sprintf("%ds", config.MaxAge))
	} else {
		config.maxAgeDuration = 0
	}
	start := func() {
		log.Println("Starting Listener")
		log.Fatal(http.ListenAndServe(":8080", nil))
	}
	if sync {
		start()
	} else {
		go start()
	}
}

func init() {
	RegisterPipelineService("Session", &AttachSession{})
	RegisterPipelineService("Auth", &Authenticate{})
	RegisterPipelineService("Log", &LogRequest{})
	RegisterPipelineService("Serve", &ServeRequest{})

	RegisterHandlerFnc("Static", serveFileHandler)
	RegisterHandlerFnc("AppIcon", AppIcon)
	RegisterHandlerFnc("JSON", JSON)
	RegisterHandlerFnc("Submit", JSONSubmit)
	RegisterHandlerFnc("SchemaAPI", tools.SchemaAPI)
	RegisterHandlerFnc("Entity", EntityPage)
	RegisterHandlerFnc("Plain", PlainPage)
	RegisterHandlerObj("Redirect", MakeRedirectHandler)
	RegisterHandlerObj("HTML", MakeStaticHandler)

	templateManager = MakeTemplateManager()
}
