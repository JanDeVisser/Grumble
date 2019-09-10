package grumble

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Column struct {
	Kind        *Kind
	FieldName   string
	Index       []int
	ColumnName  string
	VerboseName string
	IsKey       bool
	Scoped      bool
	ColumnType  ColumnType
}

type Kind struct {
	Kind          string
	TableName     string
	LabelCol      string
	Columns       []Column
	VerboseName   string
	columnsByName map[string]int
	table         *SQLTable
	typ           reflect.Type
	base          *Kind
	baseIndex     int
	derived       []*Kind
}

func (k Kind) Basename() string {
	parts := strings.Split(k.Kind, ".")
	return parts[len(parts)-1]
}

func (k *Kind) SQLTable() *SQLTable {
	if k.table == nil {
		pg := GetPostgreSQLAdapter()
		table := pg.makeTable(k.TableName)
		k.table = &table
	}
	return k.table
}

func (k Kind) QualifiedTableName() string {
	return k.SQLTable().QualifiedName()
}

func (k Kind) Name() string {
	return k.Kind
}

func (k Kind) Type() reflect.Type {
	return k.typ
}

func (k *Kind) ColumnNames() []string {
	var names = make([]string, 0)
	for _, column := range k.Columns {
		names = append(names, column.ColumnName)
	}
	return names
}

func (k *Kind) FieldNames() []string {
	var names = make([]string, 0)
	for _, column := range k.Columns {
		names = append(names, column.FieldName)
	}
	return names
}

func (k *Kind) Column(name string) (col Column, ok bool) {
	ix, ok := k.columnsByName[name]
	if !ok {
		return
	}
	col = k.Columns[ix]
	return
}

func (k *Kind) ColumnByFieldName(name string) (col Column, ok bool) {
	ok = false
	for _, col = range k.Columns {
		ok = col.FieldName == name
		if ok {
			return
		}
	}
	return
}

var RegistryByType map[reflect.Type]*Kind = make(map[reflect.Type]*Kind)
var RegistryByKind map[string]*Kind = make(map[string]*Kind)

func GetKind(obj Persistable) (kind *Kind) {
	switch e := obj.(type) {
	case *Key:
		return e.kind
	default:
		s := reflect.ValueOf(obj).Elem()
		t := s.Type()
		var ok bool
		kind, ok = RegistryByType[t]
		if !ok {
			kind = createKind(t, obj)
		}
	}
	return kind
}

func SetKind(e Persistable) *Kind {
	k := GetKind(e)
	e.SetKind(k)
	return k
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
	if kind, ok := RegistryByKind[k]; ok {
		return kind
	}
	for _, kind := range RegistryByKind {
		if kind.Basename() == k {
			return kind
		}
	}
	return nil
}

func ParseTag(tag string) map[string]string {
	ret := make(map[string]string)
	tags := strings.Split(tag, ";")
	for _, t := range tags {
		t := strings.TrimSpace(t)
		if t != "" {
			nameValue := strings.SplitN(t, "=", 2)
			name := strings.ToLower(strings.TrimSpace(nameValue[0]))
			if len(nameValue) == 2 {
				ret[name] = strings.TrimSpace(nameValue[1])
			} else {
				ret[name] = "true"
			}
		}
	}
	return ret
}

func createKind(t reflect.Type, obj interface{}) *Kind {
	persistable := reflect.TypeOf((*Persistable)(nil)).Elem()
	if !(t.Implements(persistable) || reflect.PtrTo(t).Implements(persistable)) {
		return nil
	}
	var s reflect.Value // s is the reflected value of obj. If obj is nil, we allocate a new dummy struct of type t
	if obj == nil {
		s = reflect.Indirect(reflect.New(t))
		obj = s.Interface()
	} else {
		s = reflect.ValueOf(obj).Elem()
	}

	kind := new(Kind)
	kind.Kind = strings.ReplaceAll(strings.ToLower(t.PkgPath()+"."+t.Name()), "/", ".")
	kind.TableName = kind.Basename()
	kind.Columns = make([]Column, 0)
	kind.columnsByName = make(map[string]int)
	kind.typ = t
	keyFound := false
	for i := 0; i < t.NumField(); i++ {
		fld := t.Field(i)
		objFld := s.Field(i)
		r, _ := utf8.DecodeRuneInString(fld.Name)
		if !unicode.IsUpper(r) {
			continue
		}
		tagString, ok := fld.Tag.Lookup("grumble")
		switch {
		case fld.Type.Kind() == reflect.Struct && fld.Type.Name() == "Key":
			kind.parseEntityTag(fld.Tag.Get("grumble"))
			if kind.base != nil {
				panic(fmt.Sprintf("Kind '%s' can't embed Entity directly and have a base Kind", kind.Kind))
			}
			keyFound = true
		case fld.Type.Kind() == reflect.Struct && fld.Type.String() == "time/Time":
			if ok {
				kind.AddColumn(fld, objFld, ParseTag(tagString))
			}
		case fld.Type.Kind() == reflect.Struct:
			structType := fld.Type
			structKind := GetKindForType(structType)
			if structKind != nil {
				if kind.base != nil {
					panic(fmt.Sprintf("Kind '%s': Multiple inheritance not supported.", kind.Kind))
				}
				if keyFound {
					panic(fmt.Sprintf("Kind '%s' can't embed Key directly and have a base Kind", kind.Kind))
				}
				kind.SetBaseKind(i, fld, structKind)
			}
		case fld.Type.Kind() == reflect.Ptr && fld.Type.Elem().Kind() == reflect.Struct:
			structType := fld.Type.Elem()
			structKind := GetKindForType(structType)
			if structKind != nil && ok {
				kind.AddColumn(fld, objFld, ParseTag(tagString))
			}
		case objFld.CanInterface():
			if ok {
				kind.AddColumn(fld, objFld, ParseTag(tagString))
			}
		}
	}
	if err := kind.reconcile(); err != nil {
		panic(err)
	}
	RegistryByType[t] = kind
	RegistryByKind[kind.Kind] = kind
	return kind
}

func (k *Kind) addColumn(column Column) {
	if _, ok := k.columnsByName[column.FieldName]; ok {
		panic(fmt.Sprintf("Kind '%s' cannot have two columns with the same name '%s'",
			k.Kind, column.ColumnName))
	}
	k.Columns = append(k.Columns, column)
	k.columnsByName[column.FieldName] = len(k.Columns) - 1
}

func (k *Kind) SetBaseKind(index int, field reflect.StructField, base *Kind) {
	k.parseEntityTag(field.Tag.Get("grumble"))
	k.base = base
	k.baseIndex = index
	base.AddDerivedKind(k)
	for _, column := range base.Columns {
		derivedColumn := column
		derivedColumn.Index = make([]int, 1)
		derivedColumn.Index[0] = index
		derivedColumn.Index = append(derivedColumn.Index, column.Index...)
		k.addColumn(derivedColumn)
	}
}

func (k *Kind) AddDerivedKind(derived *Kind) {
	if k.derived == nil {
		k.derived = make([]*Kind, 0)
	}
	k.derived = append(k.derived, derived)
}

func (k *Kind) DerivedKinds() (ret []*Kind) {
	ret = make([]*Kind, 0)
	todo := make([]*Kind, 0)
	current := k
	for current != nil {
		todo = append(todo, current.derived...)
		ret = append(ret, current.derived...)
		if len(todo) > 0 {
			current = todo[0]
			todo = todo[1:]
		} else {
			current = nil
		}
	}
	return
}

func (k *Kind) DerivesFrom(base *Kind) bool {
	for b := k; b != nil; b = b.base {
		if b.Kind == base.Kind {
			return true
		}
	}
	return false
}

func (k *Kind) parseEntityTag(tagstring string) {
	tags := ParseTag(tagstring)
	if v, ok := tags["tablename"]; ok {
		k.TableName = v
	}
	if v, ok := tags["verbosename"]; ok {
		k.VerboseName = v
	}
}

func (k *Kind) AddColumn(field reflect.StructField, objFld reflect.Value, tags map[string]string) {
	ret := Column{}
	ret.FieldName = field.Name
	ret.IsKey = false
	ret.Index = make([]int, 1)
	ret.Index[0] = field.Index[0]
	if v, ok := tags["key"]; ok {
		b, err := strconv.ParseBool(v)
		if err == nil && b {
			ret.IsKey = true
			ret.Scoped = true
			if v, ok := tags["scoped"]; ok {
				b, err = strconv.ParseBool(v)
				if err == nil {
					ret.Scoped = b
				}
			}
		}
	}
	if v, ok := tags["label"]; ok {
		isLabel, err := strconv.ParseBool(v)
		if err == nil && isLabel {
			k.LabelCol = ret.FieldName
		}
	}
	if v, ok := tags["columnname"]; ok {
		ret.ColumnName = v
	} else {
		ret.ColumnName = ret.FieldName
	}
	if v, ok := tags["verbosename"]; ok {
		ret.VerboseName = v
	} else {
		ret.VerboseName = string(regexp.MustCompile("([[:lower:]])([[:upper:]])").ReplaceAll(
			[]byte(strings.ReplaceAll(strings.Title(ret.FieldName), "_", " ")),
			[]byte("$1 $2")))
	}
	ret.Kind = k
	typ := tags["type"]
	switch {
	case field.Type.Kind() == reflect.Struct && field.Type.String() == "time/Time":
		if typ == "" {
			typ = "timestamp"
		}
		ret.ColumnType = &BasicColumnType{typ}
	case field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct:
		structType := field.Type.Elem()
		structKind := GetKindForType(structType)
		if structKind != nil {
			ret.ColumnType = &ReferenceType{References: structKind}
		}
	default:
		if typ != "" {
			ret.ColumnType = &BasicColumnType{typ}
		} else {
			ret.ColumnType = ColumnTypes[objFld.Kind()]
		}
	}
	k.addColumn(ret)
}

var _idColumn = SQLColumn{Name: "_id", SQLType: "serial", Default: "", Nullable: false, PrimaryKey: true, Unique: false, Indexed: false}
var _parentColumn = SQLColumn{Name: "_parent", SQLType: "text", Default: "", Nullable: true, PrimaryKey: false, Unique: false, Indexed: false}
var _parentIndex = SQLIndex{Columns: []string{"_parent", "_id"}, PrimaryKey: false, Unique: true}

func (k *Kind) reconcile() (err error) {
	table := k.SQLTable()
	if err = table.AddColumn(_idColumn); err != nil {
		return
	}
	if err = table.AddColumn(_parentColumn); err != nil {
		return
	}
	if err = table.AddIndex(_parentIndex); err != nil {
		return
	}
	for _, col := range k.Columns {
		c := SQLColumn{}
		c.Name = col.ColumnName
		c.SQLType = col.ColumnType.SQLType(col)
		if err = table.AddColumn(c); err != nil {
			return
		}
		if col.IsKey {
			keyCols := make([]string, 2)
			if col.Scoped {
				keyCols = append(keyCols, "_parent")
			}
			keyCols = append(keyCols, col.ColumnName)
			index := SQLIndex{Columns: keyCols, PrimaryKey: false, Unique: true}
			if err = table.AddIndex(index); err != nil {
				return
			}
		}
	}
	if err = table.Reconcile(); err != nil {
		return
	}
	return
}

func (k *Kind) MakeValue(parent string, id int) (value reflect.Value, entity Persistable, err error) {
	value = reflect.New(k.Type())
	entity, ok := value.Interface().(Persistable)
	if !ok {
		err = errors.New(fmt.Sprintf("Cannot instantiate kind '%s'", k.Kind))
		return
	}
	SetKind(entity)
	entity.Initialize(parent, id)
	return
}

func (k *Kind) Make(parent string, id int) (entity Persistable, err error) {
	_, entity, err = k.MakeValue(parent, id)
	return
}

func (k *Kind) New(parent *Key) (entity Persistable, err error) {
	return k.Make(parent.String(), 0)
}

func (k *Kind) Copy(src, target Persistable) (ret Persistable, err error) {
	target.Initialize(src.Parent().String(), src.Id())
	sourceValue := reflect.ValueOf(src).Elem()
	targetValue := reflect.ValueOf(target).Elem()
	for _, column := range k.Columns {
		sourceField := sourceValue.FieldByIndex(column.Index)
		targetField := targetValue.FieldByIndex(column.Index)
		targetField.Set(sourceField)
	}
	return target, nil
}
