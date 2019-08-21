package grumble

import (
	"reflect"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Column struct {
	FieldName string
	ColumnName string
	sqlType string
	VerboseName string
}

type Kind struct {
	Kind string
	TableName string
	KeyCol string
	Columns map[string]Column
}

func (k *Kind) Basename() string {
	parts := strings.Split(k.Kind, ".")
	return parts[len(parts) - 1]
}

func (k *Kind) Name() string {
	return k.Kind
}

var RegistryByType map[reflect.Type]*Kind = make(map[reflect.Type]*Kind)
var RegistryByKind map[string]*Kind = make(map[string]*Kind)
var SqlTypes = map[reflect.Kind]string{
	reflect.Bool: "BOOLEAN",
	reflect.Int: "INTEGER",
	reflect.Int8: "INTEGER",
	reflect.Int16: "INTEGER",
	reflect.Int32: "INTEGER",
	reflect.Int64: "INTEGER",
	reflect.Uint: "INTEGER",
	reflect.Uint8: "INTEGER",
	reflect.Uint16: "INTEGER",
	reflect.Uint32: "INTEGER",
	reflect.Uint64: "INTEGER",
	reflect.Float32: "FLOAT",
	reflect.Float64: "FLOAT",
	reflect.String: "TEXT",
}

func GetKind(obj interface{}) *Kind {
	s := reflect.ValueOf(obj).Elem()
	t := s.Type()
	var kind *Kind
	kind, ok := RegistryByType[t]
	if !ok {
		kind = createKind(t, obj)
	}
	return kind
}

func GetKindForType(t reflect.Type) *Kind {
	var kind *Kind
	kind, ok := RegistryByType[t]
	if !ok {
		kind = createKind(t, nil)
	}
	return kind
}

func GetKindForKind(k string) *Kind {
	k = strings.ReplaceAll(strings.ToLower(k), "/", ".")
	var kind *Kind
	var ok bool
	if kind, ok = RegistryByKind[k]; ok {
		return kind
	}
	for _, kind := range RegistryByKind {
		if kind.Basename() == k {
			return kind
		}
	}
	return nil
}

func createKind(t reflect.Type, obj interface{}) *Kind {
	kind := new(Kind)
	kind.Kind = strings.ReplaceAll(strings.ToLower(t.PkgPath() + "." + t.Name()), "/", ".")
	kind.KeyCol = "_key_name"
	kind.Columns = make(map[string]Column)
	var s reflect.Value
	if obj == nil {
		s = reflect.Indirect(reflect.New(t))
	} else {
		s = reflect.ValueOf(obj).Elem()
	}
	var canPut bool = false
	for i := 0; i < t.NumField(); i++ {
		fld := t.Field(i)
		objFld := s.Field(i)
		r, _ := utf8.DecodeRuneInString(fld.Name)
		var persist= false
		if unicode.IsUpper(r) && objFld.CanInterface() {
			val := objFld.Interface()
			if _, ok := val.(PersistableField); ok {
				persist = true
			} else if fld.Tag.Get("persistable") != "" {
				persist = true
			}
			if v := fld.Tag.Get("key"); v != "" {
				kind.KeyCol = fld.Name
			}
			if persist {
				kind.AddColumn(fld, objFld)
			}
		}
		canPut = canPut || persist
	}
	if !canPut {
		return nil
	}
	RegistryByType[t] = kind
	return kind
}

func (k *Kind) AddColumn(field reflect.StructField, objFld reflect.Value) {
	ret := Column{}
	ret.FieldName = field.Name
	if v := field.Tag.Get("column_name"); v != "" {
		ret.ColumnName = v
	} else {
		ret.ColumnName = ret.FieldName
	}
	if v := field.Tag.Get("verbose_name"); v != "" {
		ret.VerboseName = v
	} else {
		var re *regexp.Regexp = regexp.MustCompile("([[:lower:]])([[:upper:]])")
		ret.VerboseName = string(re.ReplaceAll(
			[]byte(strings.ReplaceAll(strings.Title(ret.FieldName), "_", " ")),
			[]byte("$1 $2")))
	}
	ret.sqlType = SqlTypes[objFld.Kind()]
	k.Columns[ret.FieldName] = ret
}

func (k *Kind) reconcile() bool {
	return true
}



