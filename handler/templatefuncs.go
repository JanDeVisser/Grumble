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
	"bytes"
	"fmt"
	"html/template"
	"reflect"
	"strconv"
	"strings"
	texttemplate "text/template"
	"time"

	"github.com/JanDeVisser/grumble"
)

func GetSyntheticField(e grumble.Persistable, fld string) interface{} {
	ret, ok := e.SyntheticField(fld)
	if !ok {
		ret = nil
	}
	return ret
}

func Concat(args ...interface{}) string {
	builder := strings.Builder{}
	for _, arg := range args {
		switch a := arg.(type) {
		case string:
			builder.WriteString(a)
		case fmt.Stringer:
			builder.WriteString(a.String())
		case time.Time:
			builder.WriteString(a.Format("2006-02-01"))
		case bool:
			builder.WriteString(strconv.FormatBool(a))
		case int, uint, uint64:
			builder.WriteString(fmt.Sprintf("%d", a))
		case float32, float64:
			builder.WriteString(fmt.Sprintf("%.2f", a))
		default:
			builder.WriteString(fmt.Sprintf("%v (%T)", a, a))
		}
	}
	return builder.String()
}

func MakeMap(args ...interface{}) (ret map[string]interface{}) {
	ret = make(map[string]interface{}, 0)
	key := ""
	ok := true
	for ix, arg := range args {
		if ix%2 == 0 {
			if key, ok = arg.(string); !ok {
				key = ""
			}
		} else if key != "" {
			ret[key] = arg
		}
	}
	return
}

func MakeTags(args ...interface{}) (ret *grumble.Tags) {
	ret = MakeTags()
	ret.Append(args...)
	return
}

func MakeSlice(args ...interface{}) []interface{} {
	return args
}

func IntRange(from int, upto int) []int {
	ret := make([]int, upto-from)
	for ix := from; ix < upto; ix++ {
		ret[ix-from] = ix
	}
	return ret
}

func SliceContains(slice []interface{}, value interface{}) bool {
	for _, elem := range slice {
		switch v := value.(type) {
		case string:
			ve, ok := elem.(string)
			if ok && v == ve {
				return true
			}
		case int:
			ve, ok := elem.(int)
			if ok && v == ve {
				return true
			}
		case bool:
			ve, ok := elem.(bool)
			if ok && v == ve {
				return true
			}
		case int64:
			ve, ok := elem.(int64)
			if ok && v == ve {
				return true
			}
		case float32:
			ve, ok := elem.(float32)
			if ok && v == ve {
				return true
			}
		case float64:
			ve, ok := elem.(float64)
			if ok && v == ve {
				return true
			}
		}
	}
	return false
}

func SetTabs(data interface{}, tabs interface{}) string {
	if ctx, ok := data.(map[string]interface{}); ok {
		var tabslice = make([]string, 0)
		switch t := tabs.(type) {
		case string:
			if t != "" {
				tabslice = strings.Split(t, ",")
			}
		case []string:
			tabslice = t
		}
		ctx["tabs"] = tabslice
	}
	return ""
}

func GetAttribute(obj interface{}, attr string) interface{} {
	if m, ok := obj.(map[string]interface{}); ok {
		return m[attr]
	} else {
		if v := reflect.ValueOf(obj).Elem(); v.Kind() == reflect.Struct {
			if fld := v.FieldByName(attr); fld.IsValid() {
				return fld.Interface()
			}
		}
	}
	return nil
}

type ExecKeyGetter interface {
	ExecKey() string
}

func CallTemplate(name string, data interface{}) (ret template.HTML, err error) {
	key := ""
	switch ctx := data.(type) {
	case map[string]interface{}:
		if k, ok := ctx[ExecKey]; ok {
			if key, ok = k.(string); !ok {
				key = ""
			}
		}
	case ExecKeyGetter:
		key = ctx.ExecKey()
	}
	if key != "" {
		if tmpl, ok := Executing[key]; ok {
			buf := bytes.NewBuffer([]byte{})
			err = tmpl.ExecuteTemplate(buf, name, data)
			ret = template.HTML(buf.String())
		}
	} else {
		ret = "ERROR: CONTEXT HAS NO EXECKEY"
	}
	return
}

var templateFunctions = texttemplate.FuncMap{
	"title":        strings.Title,
	"startswith":   strings.HasPrefix,
	"endswith":     strings.HasPrefix,
	"tolower":      strings.ToLower,
	"toupper":      strings.ToUpper,
	"split":        strings.Split,
	"join":         strings.Join,
	"replace":      strings.ReplaceAll,
	"concat":       Concat,
	"map":          MakeMap,
	"tags":         MakeTags,
	"label":        grumble.Label,
	"synth":        GetSyntheticField,
	"makeslice":    MakeSlice,
	"upto":         IntRange,
	"contains":     SliceContains,
	"tabs":         SetTabs,
	"field":        MakeFieldContext,
	"lookup":       MakeLookupContext,
	"parent":       MakeParentContext,
	"attribute":    GetAttribute,
	"calltemplate": CallTemplate,
	"today": func() string {
		return time.Now().Format("2006-01-02")
	},
}

func AddTemplateFunction(name string, function interface{}) {
	templateFunctions[name] = function
}
