/*
 * This file is part of Finn.
 *
 * Copyright (c) 2019 Jan de Visser <jan@finiandarcy.com>
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
	"encoding/json"
	"fmt"
	"github.com/JanDeVisser/grumble"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

type JSONRequest struct {
	mgr    *grumble.EntityManager
	Method string
	Kind   *grumble.Kind
	Id     int
	w      http.ResponseWriter
	r      *http.Request
}

func (req *JSONRequest) Execute() {
	v := reflect.ValueOf(req)
	m := v.MethodByName(req.Method)
	if m.IsValid() {
		m.Call([]reflect.Value{})
	} else {
		panic(fmt.Sprintf("Cannot serve method %q for JSON requests", req.Method))
	}
}

func Marshal(obj interface{}) (jsonText []byte, err error) {
	var toJSON interface{}
	switch v := obj.(type) {
	case grumble.Persistable:
		var marshalled map[string]interface{}
		if marshalled, err = MarshalPersistableToMap(v); err != nil {
			return
		}
		toJSON = marshalled
	case [][]grumble.Persistable:
		objs := make([][]map[string]interface{}, 0)
		for _, row := range v {
			r := make([]map[string]interface{}, 0)
			for _, o := range row {
				var marshalled map[string]interface{}
				if marshalled, err = MarshalPersistableToMap(o); err != nil {
					return
				}
				r = append(r, marshalled)
			}
			objs = append(objs, r)
		}
		toJSON = objs
	default:
		if toJSON, err = MarshalSimpleObjectToMap(v); err != nil {
			return
		}
	}
	return json.Marshal(toJSON)
}

func MarshalPersistableToMap(obj grumble.Persistable) (jsonData map[string]interface{}, err error) {
	jsonText, err := json.Marshal(obj)
	if err != nil {
		return
	}
	if err = json.Unmarshal(jsonText, &jsonData); err != nil {
		return
	}
	k := obj.Kind()
	if k == nil {
		return
	}
	jsonData["_kind"] = k.Basename()
	for name, value := range obj.SyntheticFields() {
		switch v := value.(type) {
		case grumble.Persistable:
			var marshalled map[string]interface{}
			if marshalled, err = MarshalPersistableToMap(v); err != nil {
				return
			}
			jsonData[name] = marshalled
		default:
			var marshalled interface{}
			if marshalled, err = MarshalSimpleObjectToMap(v); err != nil {
				return
			}
			jsonData[name] = marshalled
		}
	}
	m := reflect.ValueOf(obj).MethodByName("Marshal")
	if m.IsValid() {
		rv := m.Call([]reflect.Value{reflect.ValueOf(jsonData)})
		if !rv[0].IsNil() {
			err = rv[0].Interface().(error)
		}
	}
	return
}

func MarshalSimpleObjectToMap(obj interface{}) (jsonData interface{}, err error) {
	var jsonText []byte
	jsonText, err = json.Marshal(obj)
	if err != nil {
		return
	}
	if err = json.Unmarshal(jsonText, &jsonData); err != nil {
		return
	}
	if obj != nil {
		m := reflect.ValueOf(obj).MethodByName("Marshal")
		if m.IsValid() {
			rv := m.Call([]reflect.Value{reflect.ValueOf(jsonData)})
			if !rv[0].IsNil() {
				err = rv[0].Interface().(error)
			}
		}
	}
	return
}

func (req *JSONRequest) WriteJSON(jsonText []byte) {
	req.w.Header().Add("Content-type", "application/json")
	_, err := req.w.Write(jsonText)
	if err != nil {
		http.Error(req.w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = req.w.Write([]byte("\n"))
}

func (req *JSONRequest) GET() {
	var obj interface{}
	var err error
	if req.Id > 0 {
		log.Printf("JSON.GET %s.%d", req.Kind.Kind, req.Id)
		obj, err = req.mgr.Get(req.Kind, req.Id)
	} else {
		log.Printf("JSON.GET q=%q", req.r.URL.Query().Encode())
		var results [][]grumble.Persistable
		results, err = req.mgr.Query(req.Kind, req.r.URL.Query())
		if err == nil {
			log.Printf("JSON.GET len(results): %d", len(results))
			if len(results) > 0 {
				log.Printf("JSON.GET len(results[0]): %d", len(results[0]))
			}
			obj = results
		}
	}
	if err != nil {
		log.Print(err)
		http.Error(req.w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jsonText, err := Marshal(obj); err != nil {
		http.Error(req.w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		req.WriteJSON(jsonText)
	}
}

func JSON(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s JSON: %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
	mgr, err := grumble.MakeEntityManager()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s := strings.Split(r.URL.Path[1:], "/")
	kind := grumble.GetKind(s[1])
	if kind == nil {
		http.Error(w, fmt.Sprintf("Unknown kind '%s'", s[1]), http.StatusInternalServerError)
		return
	}
	var id int64 = 0
	switch {
	case len(s) == 3:
		id, err = strconv.ParseInt(s[2], 0, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case r.Form.Get("id") != "":
		id, err = strconv.ParseInt(r.Form.Get("id"), 0, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	req := &JSONRequest{mgr: mgr, Method: r.Method, Kind: kind, Id: int(id), w: w, r: r}
	req.Execute()
}

func JSONSubmit(w http.ResponseWriter, r *http.Request) {
	log.Printf("JSONSubmit: %s%s", r.URL.Path, r.URL.RawQuery)
	mgr, err := grumble.MakeEntityManager()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s := strings.Split(r.URL.Path[1:], "/")
	kind := grumble.GetKind(s[1])
	if kind == nil {
		http.Error(w, fmt.Sprintf("Unknown kind '%s'", s[1]), http.StatusInternalServerError)
		return
	}

	// Read body
	jsonText, err := ioutil.ReadAll(r.Body)
	defer func() {
		_ = r.Body.Close()
	}()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Submit: JSON text: %s", string(jsonText))

	var jsonData interface{}
	err = json.Unmarshal(jsonText, &jsonData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Submit: Unmarshalled JSON: %+v", jsonData)

	e, _ := mgr.Make(kind, grumble.ZeroKey, 0)
	m := reflect.ValueOf(e).MethodByName("Submit")
	var ret interface{}
	if m.IsValid() {
		rv := m.Call([]reflect.Value{reflect.ValueOf(jsonData)})
		if !rv[0].IsNil() {
			ret = rv[0].Interface()
		} else {
			ret = true
		}
	}

	jsonText, err = json.Marshal(ret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-type", "application/json")
	_, err = w.Write(jsonText)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = w.Write([]byte("\n"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}