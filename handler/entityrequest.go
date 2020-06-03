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
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/JanDeVisser/grumble"
)

type EntityRequest struct {
	Manager  *grumble.EntityManager
	Method   string
	Kind     *grumble.Kind
	Id       int
	Mode     string
	Template string
	Data     interface{}
	Values   url.Values
	w        http.ResponseWriter
	r        *http.Request
}

type Initializer interface {
	InitializeNew() error
}

func NewEntityRequest(mgr *grumble.EntityManager, w http.ResponseWriter, r *http.Request) (ret *EntityRequest, err error) {
	ret = &EntityRequest{Manager: mgr, w: w, r: r}
	ret.Method = r.Method
	s := strings.Split(r.URL.Path[1:], "/")
	if len(s) > 0 {
		ret.Kind = grumble.GetKind(s[0])
	}

	var id int64 = 0
	switch {
	case ret.Kind == nil:
		break
	case len(s) > 2:
		err = errors.New("malformed request")
		return
	case len(s) == 2 && s[1] == "new":
		ret.Id = 0
		ret.Mode = "new"
		return
	case len(s) == 2:
		id, err = strconv.ParseInt(s[1], 0, 0)
		if err != nil {
			return
		}
		ret.Id = int(id)
	case r.FormValue("Id") != "":
		id, err = strconv.ParseInt(r.FormValue("Id"), 0, 0)
		if err != nil {
			return
		}
		ret.Id = int(id)
	case len(s) == 1:
		ret.Id = -1
	default:
		err = errors.New("malformed request")
		return
	}
	ret.Values = ret.r.URL.Query()
	if ret.Id > 0 && r.Method == "POST" {
		ret.Mode = "edit"
		if r.FormValue("mode") != "" {
			ret.Mode = r.FormValue("mode")
		}
	}
	return
}

func (req *EntityRequest) Execute() {
	v := reflect.ValueOf(req)
	m := v.MethodByName(req.Method)
	if m.IsValid() {
		m.Call([]reflect.Value{})
	} else {
		panic(fmt.Sprintf("Cannot serve method %q for entity requests", req.Method))
	}
}

func (req *EntityRequest) makeContext(e grumble.Persistable, p grumble.Persistable) (err error) {
	data := make(map[string]interface{}, 0)
	switch {
	case req.r.FormValue("mode") != "":
		data["Mode"] = req.r.FormValue("mode")
	case req.Id == 0:
		data["Mode"] = "new"
	default:
		data["Mode"] = "view"
	}
	data["URL"] = req.r.URL.Path
	data["CancelURL"] = ""
	if req.r.FormValue("cancelurl") != "" {
		data["CancelURL"] = req.r.FormValue("cancelurl")
	}
	data["Ident"] = e.Id()
	data["Kind"] = e.Kind()
	data["Entity"] = e
	data["Parameters"] = req.Values
	data[req.Kind.Basename()] = e
	if req.Kind.ParentKind != nil {
		data[req.Kind.ParentKind.Basename()] = p
		data["Parent"] = p
		data["ParentIdent"] = p.Id()
	} else {
		data["ParentIdent"] = 0
	}
	m := reflect.ValueOf(e).MethodByName("MakeContext")
	if m.IsValid() {
		rv := m.Call([]reflect.Value{reflect.ValueOf(data)})
		if !rv[0].IsNil() {
			err = rv[0].Interface().(error)
		}
	}
	req.Data = data
	req.Template = fmt.Sprintf("html/%s/view.html", req.Kind.Basename())
	return
}

func AddChildrenToContext(e grumble.Persistable, ctx map[string]interface{}, kind interface{}, tag string,
	sort *grumble.Sort) (err error) {
	ctx[tag] = make([][]grumble.Persistable, 0)
	if ctx["Mode"] == "view" {
		q := e.Manager().MakeQuery(kind)
		q.WithDerived = true
		q.HasParent(e)
		if sort != nil {
			q.AddSort(*sort)
		}
		q.AddReferenceJoins()
		results, err := q.Execute()
		if err != nil {
			return err
		}
		for ix, row := range results {
			if len(row) == 1 {
				if ix == 0 {
					ctx[tag] = make([]grumble.Persistable, 0)
				}
				children := ctx[tag].([]grumble.Persistable)
				children = append(children, row[0])
				ctx[tag] = children
			} else {
				children := ctx[tag].([][]grumble.Persistable)
				children = append(children, row)
				ctx[tag] = children
			}
		}
	}
	return
}

func (req *EntityRequest) GET() {
	var err error
	req.Template = "html/index.html"
	req.Data = nil

	action := req.r.FormValue("action")
	if req.Kind != nil {
		switch {
		case action == "delete" && req.Id > 0:
			e, err := req.Manager.Get(req.Kind, req.Id)
			if err != nil {
				http.Error(req.w, err.Error(), http.StatusInternalServerError)
				return
			}
			redirURL := ""
			if e.Parent() != nil {
				redirURL = fmt.Sprintf("/%s/%d", e.Parent().Kind().Basename(), e.Parent().Id())
			}
			if req.r.FormValue("redirect") != "" {
				redirURL = req.r.FormValue("redirect")
			}
			if err = req.Manager.Delete(e); err != nil {
				http.Error(req.w, err.Error(), http.StatusInternalServerError)
				return
			}
			if redirURL != "" {
				http.Redirect(req.w, req.r, redirURL, http.StatusFound)
			}
		case req.Id > 0:
			log.Printf("Entity.GET %s.%d", req.Kind.Kind, req.Id)
			q := req.Manager.MakeQuery(req.Kind)
			q.AddCondition(&grumble.HasId{Id: req.Id})
			if req.Kind.ParentKind != nil {
				q.AddParentJoin(req.Kind.ParentKind)
			}
			q.AddReferenceJoins()
			var results [][]grumble.Persistable
			results, err = q.Execute()
			switch {
			case err != nil:
				break
			case len(results) == 0:
				http.Error(req.w,
					fmt.Sprintf("%s:%d not found", req.Kind.Basename(), req.Id),
					http.StatusNotFound)
				return
			case len(results) > 1:
				http.Error(req.w,
					fmt.Sprintf("%s:%d ambiguous", req.Kind.Basename(), req.Id),
					http.StatusInternalServerError)
				return
			default:
				var p grumble.Persistable
				if req.Kind.ParentKind != nil {
					p = results[0][1]
				}
				err = req.makeContext(results[0][0], p)
			}
		case req.Id == 0: // mode=new:
			var p grumble.Persistable
			pkey := grumble.ZeroKey
			if req.Kind.ParentKind != nil {
				parentIdStr := req.r.FormValue("pid")
				if parentIdStr == "" {
					http.Error(req.w,
						fmt.Sprintf("%s: no pid in new request", req.Kind.Basename()),
						http.StatusInternalServerError)
				}
				pid, err := strconv.ParseInt(parentIdStr, 0, 0)
				if err != nil {
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
					return
				}
				p, err = req.Manager.Get(req.Kind.ParentKind, int(pid))
				if err != nil {
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
					return
				}
				pkey = p.AsKey()
			}
			blank, err := req.Manager.New(req.Kind, pkey)
			if err != nil {
				http.Error(req.w, err.Error(), http.StatusInternalServerError)
				return
			}
			if initializer, ok := blank.(Initializer); ok {
				if err = initializer.InitializeNew(); err != nil {
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
			err = req.makeContext(blank, p)
		default: // No Id. list mode
			log.Printf("Entity.GET q=%q", req.r.URL.Query().Encode())
			var results [][]grumble.Persistable
			results, err = req.Manager.Query(req.Kind, req.r.URL.Query())
			if err == nil {
				log.Printf("Entity.GET len(results): %d", len(results))
				if len(results) > 0 {
					log.Printf("Entity.GET len(results[0]): %d", len(results[0]))
				}
				e, err := req.Kind.New(nil)
				if err != nil {
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
					return
				}
				req.Template = fmt.Sprintf("html/%s/list.html", req.Kind.Basename())
				m := reflect.ValueOf(e).MethodByName("MakeListContext")
				if m.IsValid() {
					data := make(map[string]interface{}, 1)
					data["Parameters"] = req.Values
					data["URL"] = req.r.URL.Path
					data["Kind"] = req.Kind

					data["results"] = results
					rv := m.Call([]reflect.Value{reflect.ValueOf(req), reflect.ValueOf(data)})
					if !rv[0].IsNil() {
						err = rv[0].Interface().(error)
					}
					req.Data = data
				} else {
					req.Data = results
				}
			}
		}
	} else {
		http.Redirect(req.w, req.r, "/index.html", http.StatusFound)
		return
	}
	if err != nil {
		log.Print(err)
		http.Error(req.w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.serveTemplate()
}

func (req *EntityRequest) POST() {
	var err error
	req.Template = "html/index.html"
	req.Data = nil

	if req.Kind != nil && req.Id >= 0 {
		log.Printf("Entity.POST %s.%d", req.Kind.Kind, req.Id)
		var entity grumble.Persistable
		if req.Id > 0 {
			entity, err = req.Manager.Get(req.Kind, req.Id)
		} else {
			pkey := grumble.ZeroKey
			pkind := req.Kind.ParentKind
			if req.r.FormValue("pkind") != "" {
				pkind = grumble.GetKind(req.r.FormValue("pkind"))
				if pkind == nil {
					err = errors.New(fmt.Sprintf("%s: invalid pkind %q in new request",
						req.Kind.Basename(), req.r.FormValue("pkind")))
					log.Print(err)
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
				}
			}
			if pkind != nil {
				parentIdStr := req.r.FormValue("pid")
				if parentIdStr == "" {
					err = errors.New(fmt.Sprintf("%s: no pid in new request", req.Kind.Basename()))
					log.Print(err)
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
				}
				pid, err := strconv.ParseInt(parentIdStr, 0, 0)
				if err != nil {
					log.Print(err)
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
					return
				}
				p, err := req.Manager.Get(req.Kind.ParentKind, int(pid))
				if err != nil {
					log.Print(err)
					http.Error(req.w, err.Error(), http.StatusInternalServerError)
					return
				}
				pkey = p.AsKey()
			}
			entity, err = req.Manager.New(req.Kind, pkey)
		}
		if err != nil {
			log.Print(err)
			http.Error(req.w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err = req.r.ParseForm(); err != nil {
			log.Print(err)
			http.Error(req.w, err.Error(), http.StatusInternalServerError)
			return
		}
		attribs := make(map[string]interface{})
		for field, value := range req.r.Form {
			r, _ := utf8.DecodeRuneInString(field)
			if !unicode.IsUpper(r) {
				continue
			}
			attribs[field] = value[0]
		}

		v := reflect.ValueOf(entity)
		m := v.MethodByName(fmt.Sprintf("Post%s", strings.Title(req.Mode)))
		if m.IsValid() {
			out := m.Call([]reflect.Value{reflect.ValueOf(attribs)})
			if out != nil && len(out) > 0 {
				outval := out[len(out)-1].Interface()
				if outval != nil {
					err = outval.(error)
				}
			}
		} else {
			if req.Mode == "edit" || req.Mode == "new" {
				entity, err = grumble.Populate(entity, attribs)
				if err == nil {
					err = entity.Manager().Put(entity)
				}
			} else {
				panic(fmt.Sprintf("Cannot serve method %q for entity requests", req.Method))
			}
		}

		if err != nil {
			log.Print(err)
			http.Error(req.w, err.Error(), http.StatusInternalServerError)
			return
		}
		redirURL := fmt.Sprintf("/%s/%d", entity.Kind().Basename(), entity.Id())
		if req.r.FormValue("redirect") != "" {
			redirURL = req.r.FormValue("redirect")
		}
		http.Redirect(req.w, req.r, redirURL, http.StatusFound)
	} else {
		log.Print(err)
		http.Error(req.w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (req *EntityRequest) serveTemplate() {
	templateManager.Serve(req.Template, req.Data, EntityTmpl, req.w, req.r)
}

func EntityPage(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s Page: %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
	mgr, err := grumble.MakeEntityManager()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req, err := NewEntityRequest(mgr, w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		req.Execute()
	}
}

type BaseContext struct {
	Ctx      map[string]interface{}
	Mode     string
	Template string
	Label    string
	Value    string
	HRef     string
	Tags     *grumble.Tags
	execKey  string
}

func MakeBaseContext(data map[string]interface{}, args ...interface{}) (ret *BaseContext) {
	ret = &BaseContext{
		Ctx:     data,
		Mode:    data["Mode"].(string),
		Label:   "",
		Tags:    grumble.MakeTags(),
		execKey: data[ExecKey].(string),
	}
	ret.Tags.Append(args...)
	if ret.Tags.Has("label") {
		ret.Label = ret.Tags.Get("label")
	}
	return
}

func (ctx *BaseContext) ExecKey() string {
	return ctx.execKey
}

type BasicFieldContext struct {
	BaseContext
	InputType string
	Entity    grumble.Persistable
	Column    grumble.Column
	FieldName string
}

func MakeBasicFieldContext(data map[string]interface{}, e grumble.Persistable, col grumble.Column, args ...interface{}) (ret *BasicFieldContext) {
	ret = &BasicFieldContext{
		BaseContext: *MakeBaseContext(data, args...),
		Entity:      e,
		Column:      col,
		FieldName:   col.FieldName,
	}
	if ret.Label == "" {
		ret.Label = col.VerboseName
	}
	ret.Tags.Merge(col.Tags)
	if converter, ok := col.Converter.(*grumble.BasicConverter); ok {
		switch {
		case converter.GoType.Kind() == reflect.String:
			ret.Template = "BasicField"
			ret.InputType = "text"
			if long, ok := ret.Tags.GetBool("text"); ok && long {
				ret.Template = "TextField"
			}
			if val, ok := grumble.Field(e, col.FieldName); ok {
				if ret.Value, ok = val.(string); !ok {
					ret.Value = ""
				}
			}
		case converter.GoType.Kind() >= reflect.Int && converter.GoType.Kind() <= reflect.Uint64:
			ret.Template = "BasicField"
			ret.InputType = "number"
			if val, ok := grumble.Field(e, col.FieldName); ok {
				value := reflect.ValueOf(val)
				if converter.GoType.Kind() >= reflect.Int && converter.GoType.Kind() <= reflect.Int64 {
					ret.Value = strconv.FormatInt(value.Int(), 10)
				} else if converter.GoType.Kind() >= reflect.Uint && converter.GoType.Kind() <= reflect.Uint64 {
					ret.Value = strconv.FormatUint(value.Uint(), 10)
				}
			}
		case converter.GoType.Kind() >= reflect.Float32 && converter.GoType.Kind() <= reflect.Float64:
			ret.Template = "FloatField"
			ret.InputType = "number"
			ret.Tags.Set("step", 0.01)
			if val, ok := grumble.Field(e, col.FieldName); ok {
				value := reflect.ValueOf(val)
				ret.Value = strconv.FormatFloat(value.Float(), 'f', -1, 64)
			}
		case converter.GoType == reflect.TypeOf(time.Time{}):
			ret.Template = "BasicField"
			ret.InputType = "date" // FIXME Can be time as well
			if val, ok := grumble.Field(e, col.FieldName); ok {
				if date, ok := val.(time.Time); ok {
					if ret.Mode == "view" {
						ret.Value = date.Format("Mon Jan 2, 2006")
					} else {
						ret.Value = date.Format("2006-01-02")
					}
				}
			}
		}
		if ret.Tags.Has("inputtype") {
			ret.InputType = ret.Tags.Get("inputtype")
		}
		if ret.InputType == "url" {
			ret.HRef = ret.Value
		}
		if ret.InputType == "email" {
			ret.HRef = "mailto:" + ret.Value
		}
	}
	return
}

type LookupContext struct {
	BasicFieldContext
	Reference     grumble.Persistable
	ReferenceJSON string
	Query         string
	DisplayExpr   string
}

func MakeLookupContext(basicCtx *BasicFieldContext) (ret *LookupContext) {
	ret = &LookupContext{}
	ret.BasicFieldContext = *basicCtx
	ret.Template = "Lookup"
	if refConv, ok := ret.Column.Converter.(*grumble.ReferenceConverter); ok {
		refKind := refConv.References
		ret.Tags.Merge(refKind.Tags)
		lookupField := refKind.LabelCol
		ret.ReferenceJSON = "null"
		ref, ok := grumble.Field(ret.Entity, ret.FieldName)
		var err error
		if !ok {
			ref, err = ret.Entity.Manager().New(refKind, nil)
		}
		if err == nil {
			if ret.Reference, ok = ref.(grumble.Persistable); ok {
				json, _ := Marshal(ret.Reference)
				ret.ReferenceJSON = string(json)
				ret.Value = grumble.Label(ret.Reference)
			}
			ret.HRef = fmt.Sprintf("/%s/%d", refKind.Basename(), ret.Reference.Id())
			if ret.Tags.Has("href") {
				ret.HRef, _ = url.QueryUnescape(refKind.Tags.Get("href"))
			}
		}
		ret.Query = fmt.Sprintf("_re=true&%s=", lookupField)
		if ret.Tags.Has("query") {
			ret.Query, _ = url.QueryUnescape(refKind.Tags.Get("query"))
		}
		ret.DisplayExpr = fmt.Sprintf("${e['%s']}", lookupField)
		if ret.Tags.Has("display") {
			ret.DisplayExpr, _ = url.QueryUnescape(refKind.Tags.Get("display"))
		}
	}
	return
}

func MakeFieldContext(data map[string]interface{}, e grumble.Persistable, field string, args ...interface{}) (ret interface{}) {
	k := e.Kind()
	if col, ok := k.ColumnByFieldName(field); !ok {
		return nil
	} else {
		basicCtx := MakeBasicFieldContext(data, e, col, args...)
		switch converter := col.Converter.(type) {
		case nil:
			return nil
		case *grumble.ReferenceConverter:
			//fmt.Println(converter)
			_ = converter
			return MakeLookupContext(basicCtx)
		case *grumble.BasicConverter:
			return basicCtx
		}
	}
	return nil
}

type ParentContext struct {
	BaseContext
}

func MakeParentContext(data map[string]interface{}, e grumble.Persistable, args ...interface{}) (ret *ParentContext) {
	ret = &ParentContext{BaseContext: *MakeBaseContext(data, args...)}
	if parent, err := e.Manager().Get(e.Parent().Kind(), e.Parent().Id()); err == nil {
		if ret.Label == "" {
			ret.Label = parent.Kind().VerboseName
		}
		if ret.Tags.Has("href") {
			ret.HRef = ret.Tags.Get("href")
		} else {
			ret.HRef = fmt.Sprintf("/%s/%d", parent.Kind().Basename(), parent.Id())
		}
		ret.Value = grumble.Label(parent)
	}
	return
}
