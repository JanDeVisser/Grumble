package grumble

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Column struct {
	Kind        *Kind
	FieldName   string
	Index       []int
	ColumnName  string
	Formula     string
	VerboseName string
	IsKey       bool
	Scoped      bool
	Required    bool
	Converter   Converter
	Tags        *Tags
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
	BaseKind      *Kind
	baseIndex     int
	derived       []*Kind
	ParentKind    *Kind
	Tags          *Tags
	Transient     map[string][]int
}

func (k Kind) Basename() string {
	parts := strings.Split(k.Kind, ".")
	return parts[len(parts)-1]
}

func (k *Kind) SQLTable(pg *PostgreSQLAdapter) *SQLTable {
	if k.table == nil {
		if pg == nil {
			pg = GetPostgreSQLAdapter()
		}
		table := pg.makeTable(k.TableName)
		k.table = &table
	} else if pg != nil {
		k.table.pg = *pg
	}
	return k.table
}

func (k Kind) QualifiedTableName() string {
	return k.SQLTable(nil).QualifiedName()
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

func (k *Kind) Transients() []string {
	ret := make([]string, 0)
	for n, _ := range k.Transient {
		ret = append(ret, n)
	}
	return ret
}

func (k *Kind) IsTransient(fld string) bool {
	_, ok := k.Transient[fld]
	return ok
}

var RegistryByType = make(map[reflect.Type]*Kind)
var RegistryByKind = make(map[string]*Kind)

func Kinds() []*Kind {
	ret := make([]*Kind, 0)
	for _, k := range RegistryByType {
		ret = append(ret, k)
	}
	return ret
}

func probeKind(obj interface{}) (kind *Kind) {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return getKindForType(v.Type())
}

func GetKind(obj interface{}) (kind *Kind) {
	switch e := obj.(type) {
	case Persistable:
		kind = e.Kind()
		if kind == nil {
			kind = probeKind(obj)
		}
		return
	case *Key:
		return e.kind
	case Key:
		return e.kind
	case string:
		return getKindForKind(e)
	case reflect.Type:
		return getKindForType(e)
	case *Kind:
		return e
	case Kind:
		return RegistryByKind[e.Kind]
	default:
		return probeKind(obj)
	}
}

func SetKind(e Persistable) *Kind {
	k := GetKind(e)
	e.SetKind(k)
	if e.Parent() != nil {
		e.Initialize(e.Parent(), e.Id())
	}
	return k
}

func Initialize(e Persistable, parent *Key, id int) Persistable {
	e.Initialize(parent, id)
	if e.Kind() == nil {
		SetKind(e)
	}
	return e
}

func getKindForType(t reflect.Type) *Kind {
	var kind *Kind
	kind, ok := RegistryByType[t]
	if !ok {
		kind = createKind(t, nil)
	}
	return kind
}

func getKindForKind(k string) *Kind {
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

var persistable reflect.Type = nil
var typeAdapter reflect.Type = nil

var DoReconcile = true

func createKind(t reflect.Type, obj interface{}) *Kind {
	if persistable == nil {
		persistable = reflect.TypeOf((*Persistable)(nil)).Elem()
		typeAdapter = reflect.TypeOf((*Adapter)(nil)).Elem()
	}
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
	kind.Transient = make(map[string][]int)
	keyFound := false
	RegistryByType[t] = kind
	RegistryByKind[kind.Kind] = kind
	for i := 0; i < t.NumField(); i++ {
		fld := t.Field(i)
		r, _ := utf8.DecodeRuneInString(fld.Name)
		if !unicode.IsUpper(r) {
			continue
		}
		tags := ParseTags(fld.Tag.Get("grumble"))
		if transient, _ := tags.GetBool("transient"); transient {
			kind.Transient[fld.Name] = []int{i}
			continue
		}
		switch {
		case fld.Type.Kind() == reflect.Struct && (fld.Type.Name() == "Key" || fld.Type.Name() == "grumble/Key"):
			if kind.BaseKind != nil {
				panic(fmt.Sprintf("Kind '%s' can't embed Key directly and have a BaseKind Kind", kind.Kind))
			}
			kind.parseEntityTags(tags)
			keyFound = true
			continue
		case fld.Type.Kind() == reflect.Struct:
			structKind := GetKind(fld.Type)
			if structKind != nil {
				if kind.BaseKind != nil {
					panic(fmt.Sprintf("Kind '%s': Multiple inheritance not supported.", kind.Kind))
				}
				if keyFound {
					panic(fmt.Sprintf("Kind '%s' can't embed Key directly and have a BaseKind Kind", kind.Kind))
				}
				kind.SetBaseKind(i, structKind, tags)
				continue
			}
		}
		if converter := kind.GetConverter(fld, tags); converter != nil {
			kind.CreateColumn(fld, converter, tags)
		}
	}
	if DoReconcile {
		if err := kind.Reconcile(nil); err != nil {
			panic(err)
		}
	}
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

func (k *Kind) SetBaseKind(index int, base *Kind, tags *Tags) {
	k.parseEntityTags(tags)
	k.BaseKind = base
	k.baseIndex = index
	base.AddDerivedKind(k)
	if k.ParentKind == nil {
		k.ParentKind = base.ParentKind
	}
	for _, column := range base.Columns {
		derivedColumn := column
		derivedColumn.Index = make([]int, 1)
		derivedColumn.Index[0] = index
		derivedColumn.Index = append(derivedColumn.Index, column.Index...)
		k.addColumn(derivedColumn)
	}
	for n, baseIndex := range base.Transient {
		ix := make([]int, 1)
		ix[0] = index
		ix = append(ix, baseIndex...)
		k.Transient[n] = ix
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
	for b := k; b != nil; b = b.BaseKind {
		if b.Kind == base.Kind {
			return true
		}
	}
	return false
}

func (k *Kind) parseEntityTags(tags *Tags) {
	if tags == nil {
		return
	}
	if tags.Has("tablename") {
		k.TableName = tags.Get("tablename")
	}
	if tags.Has("verbosename") {
		k.VerboseName = tags.Get("verbosename")
	} else {
		k.VerboseName = strings.Title(k.Basename())
	}
	if tags.Has("parentkind") {
		k.ParentKind = getKindForKind(tags.Get("parentkind"))
	}
	k.Tags = tags
}

func (k *Kind) GetConverter(field reflect.StructField, tags *Tags) (converter Converter) {
	taggedType := tags.Get("type")
	switch {
	case field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct:
		structKind := getKindForType(field.Type.Elem())
		if structKind != nil {
			converter = &ReferenceConverter{References: structKind}
		}
	case field.Type.Implements(typeAdapter):
		instance := reflect.New(field.Type).Interface()
		adapt := instance.(Adapter)
		converter = adapt.Converter(field, tags)
	case taggedType != "":
		converter = &BasicConverter{taggedType, field.Type}
	default:
		converter = GetConverterForType(field.Type)
	}
	return converter
}

func (k *Kind) CreateColumn(field reflect.StructField, converter Converter, tags *Tags) {
	column := Column{}
	column.FieldName = field.Name
	column.IsKey = false
	column.Index = make([]int, 1)
	column.Index[0] = field.Index[0]
	column.Converter = converter
	column.Required = true
	column.Tags = tags
	if v, ok := tags.GetBool("key"); ok && v {
		column.IsKey = true
		column.Scoped = true
		if v, ok = tags.GetBool("scoped"); ok {
			column.Scoped = v
		}
	}
	if v, ok := tags.GetBool("required"); ok {
		column.Required = v
	}
	if v, ok := tags.GetBool("label"); ok && v {
		k.LabelCol = column.FieldName
	}
	if tags.Has("columnname") {
		column.ColumnName = tags.Get("columnname")
	} else {
		column.ColumnName = column.FieldName
	}
	column.Formula = tags.Get("formula")
	if tags.Has("verbosename") {
		column.VerboseName = tags.Get("verbosename")
	} else {
		column.VerboseName = string(regexp.MustCompile("([[:lower:]])([[:upper:]])").ReplaceAll(
			[]byte(strings.ReplaceAll(strings.Title(column.FieldName), "_", " ")),
			[]byte("$1 $2")))
	}
	column.Kind = k
	k.addColumn(column)
}

var _idColumn = SQLColumn{Name: "_id", SQLType: "serial", Default: "", Nullable: false, PrimaryKey: true, Unique: false, Indexed: false}
var _parentColumn = SQLColumn{Name: "_parent", SQLType: "", Default: "", Nullable: true, PrimaryKey: false, Unique: false, Indexed: false}
var _parentIndex = SQLIndex{Columns: []string{"_parent", "_id"}, PrimaryKey: false, Unique: true}

func (k *Kind) Reconcile(pg *PostgreSQLAdapter) (err error) {
	table := k.SQLTable(pg)
	if table.GetColumnByName(_idColumn.Name) == nil {
		if err = table.AddColumn(_idColumn); err != nil {
			return
		}
		if _parentColumn.SQLType == "" {
			_parentColumn.SQLType = fmt.Sprintf("%q.\"Reference\"[]", table.pg.Schema)
		}
		if err = table.AddColumn(_parentColumn); err != nil {
			return
		}
		if err = table.AddIndex(_parentIndex); err != nil {
			return
		}
		for _, col := range k.Columns {
			if col.Formula == "" {
				c := SQLColumn{}
				c.Name = col.ColumnName
				c.SQLType = col.Converter.SQLType(col)
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
		}
	}
	if err = table.Reconcile(); err != nil {
		return
	}
	return
}

func (k *Kind) Truncate(pg *PostgreSQLAdapter) error {
	return k.SQLTable(pg).Truncate()
}

func (k *Kind) MakeValue(parent *Key, id int) (value reflect.Value, entity Persistable, err error) {
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

func (k *Kind) Make(parent *Key, id int) (entity Persistable, err error) {
	_, entity, err = k.MakeValue(parent, id)
	return
}

func (k *Kind) New(parent *Key) (entity Persistable, err error) {
	return k.Make(parent, 0)
}
