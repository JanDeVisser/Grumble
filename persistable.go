package grumble

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

type Persistable interface {
	Initialize(*Key, int) *Key
	SetKind(*Kind)
	Kind() *Kind
	Parent() *Key
	AsKey() *Key
	Id() int
	Self() (Persistable, error)
	Field(string) interface{}
	SetField(string, interface{})
	Populated() bool
	SetPopulated()
	SyntheticField(string) (interface{}, bool)
	SetSyntheticField(string, interface{})
}

// --------------------------------------------------------------------------

func SetField(e Persistable, fieldName string, value interface{}) (ok bool) {
	col, ok := e.Kind().Column(fieldName)
	if !ok {
		return
	}
	v := reflect.ValueOf(e).Elem()
	fld := v.FieldByIndex(col.Index)
	if fld.IsValid() && fld.CanSet() {
		fld.Set(reflect.ValueOf(value))
	}
	return
}

func CastTo(e Persistable, kind *Kind) Persistable {
	if !e.Kind().DerivesFrom(kind) {
		return e
	}
	v := reflect.ValueOf(e).Elem()
	ret := e
	for k := e.Kind(); k.Kind != kind.Kind; k = k.BaseKind {
		baseFld := v.Field(ret.Kind().baseIndex)
		ret = baseFld.Addr().Interface().(Persistable)
	}
	return ret
}

func Copy(src, target Persistable) (ret Persistable, err error) {
	if target.Kind() == nil {
		SetKind(target)
	}
	tk := target.Kind()
	sk := src.Kind()
	kind := sk
	var srcP, targetP Persistable
	switch {
	case tk == nil || sk == nil:
		return
	case tk.Kind == sk.Kind:
		srcP = src
		targetP = target
	case tk.DerivesFrom(sk):
		targetP = CastTo(target, sk)
		srcP = src
	case sk.DerivesFrom(tk):
		targetP = target
		srcP = CastTo(src, tk)
		kind = tk
	default:
		return
	}
	target.Initialize(src.Parent(), src.Id())
	sourceValue := reflect.ValueOf(srcP).Elem()
	targetValue := reflect.ValueOf(targetP).Elem()
	for _, col := range kind.Columns {
		targetField := targetValue.FieldByIndex(col.Index)
		if targetField.IsValid() && targetField.CanSet() {
			sourceField := sourceValue.FieldByIndex(col.Index)
			targetField.Set(sourceField)
		}
	}
	return target, nil
}

func CopyOriginal(src, target Persistable) (ret Persistable, err error) {
	if target.Kind() == nil {
		SetKind(target)
	}
	tk := target.Kind()
	sk := src.Kind()
	target.Initialize(src.Parent(), src.Id())
	sourceValue := reflect.ValueOf(src).Elem()
	targetValue := reflect.ValueOf(target).Elem()
	for _, tc := range tk.Columns {
		targetField := targetValue.FieldByIndex(tc.Index)
		if targetField.IsValid() && targetField.CanSet() {
			if sc, ok := sk.Column(tc.ColumnName); ok {
				sourceField := sourceValue.FieldByIndex(sc.Index)
				targetField.Set(sourceField)
			}
		}
	}
	return target, nil
}

func Populate(e Persistable, values map[string]interface{}) (ret Persistable, err error) {
	k := e.Kind()
	v := reflect.ValueOf(e).Elem()
	for name, value := range values {
		column, ok := k.Column(name)
		switch {
		case ok && value != nil:
			if setter, ok := column.Converter.(Setter); ok {
				err = setter.SetValue(e, column, value)
				if err != nil {
					return
				}
			} else {
				field := v.FieldByIndex(column.Index)
				field.Set(reflect.ValueOf(value))
			}
		case !ok:
			field := v.FieldByName(name)
			if field.IsValid() {
				field.Set(reflect.ValueOf(value))
			} else {
				e.SetSyntheticField(name, value)
			}
		}
	}
	e.SetPopulated()
	ret = e
	return
}

// -- P E R S I S T A N C E -------------------------------------------------

type EntityManager struct {
	*PostgreSQLAdapter
}

func MakeEntityManager() (mgr *EntityManager, err error) {
	mgr = new(EntityManager)
	mgr.PostgreSQLAdapter = GetPostgreSQLAdapter()
	return
}

func (mgr *EntityManager) Get(e Persistable, id int) (ret Persistable, err error) {
	if id <= 0 {
		err = errors.New("cannot Get() entity with ID less than or equal to zero")
		return
	}
	query := mgr.MakeQuery(e)
	query.AddCondition(HasId{Id: id})
	query.AddReferenceJoins()
	ret, err = query.ExecuteSingle(e)
	if err != nil {
		return
	}
	return
}

func (mgr *EntityManager) Inflate(e Persistable) (err error) {
	_, err = mgr.Get(e, e.Id())
	return
}

var updateEntity = SQLTemplate{Name: "UpdateEntity", SQL: `UPDATE {{.QualifiedTableName}}
	SET {{range $i, $c := .Columns}}{{if gt $i 0}},{{end}} {{if not .Formula}}"{{$c.ColumnName}}" = {{$c.Converter.SQLTextOut .}}{{end}}{{end}}
	WHERE "_id" = __count__
`}

func update(e Persistable, conn *sql.DB) (err error) {
	if !e.Populated() {
		err = errors.New("cannot update entity. It is not loaded")
	}
	k := e.Kind()
	var sqlText string
	sqlText, err = updateEntity.Process(k)
	if err != nil {
		return
	}
	values := make([]interface{}, 0)
	for _, column := range k.Columns {
		if column.Formula == "" {
			columnValues, err := column.Converter.Value(e, column)
			if err != nil {
				return err
			}
			values = append(values, columnValues...)
		}
	}
	values = append(values, e.Id())
	if _, err = conn.Exec(sqlText, values...); err != nil {
		return
	}
	return
}

var insertEntity = SQLTemplate{Name: "InsertNewEntity", SQL: `INSERT INTO {{.QualifiedTableName}}
	( "_parent"{{range $i, $c := .Columns}}{{if not .Formula}}, "{{$c.ColumnName}}"{{end}}{{end}} )
	VALUES
	( __count__{{range .Columns}}{{if not .Formula}}, {{.Converter.SQLTextOut .}}{{end}}{{end}} )
	RETURNING "_id"
`}

func insert(e Persistable, conn *sql.DB) (err error) {
	k := e.Kind()
	var sqlText string
	sqlText, err = insertEntity.Process(k)
	if err != nil {
		return
	}
	values := make([]interface{}, 0)
	values = append(values, e.AsKey().Parent().Chain())
	for _, column := range k.Columns {
		if column.Formula == "" {
			columnValues, err := column.Converter.Value(e, column)
			if err != nil {
				return err
			}
			values = append(values, columnValues...)
		}
	}
	row := conn.QueryRow(sqlText, values...)
	var id int
	err = row.Scan(&id)
	switch {
	case err == sql.ErrNoRows:
		err = errors.New("insert did not return assigned key")
		return
	case err != nil:
		return
	}
	e.Initialize(e.Parent(), id)
	e.SetPopulated()
	return
}

func (mgr *EntityManager) Put(e Persistable) (err error) {
	SetKind(e)
	return mgr.TX(func(db *sql.DB) error {
		if e.Id() > 0 {
			return update(e, db)
		} else {
			return insert(e, db)
		}
	})
}

func (mgr *EntityManager) MakeQuery(kind interface{}) *Query {
	k := GetKind(kind)
	if kind == nil {
		panic(fmt.Sprintf("Cannot create query for '%v'", kind))
	}
	query := new(Query)
	query.Kind = k
	query.Query = query
	query.mgr = mgr
	return query
}

func (mgr *EntityManager) By(kind interface{}, columnName string, value interface{}) (entity Persistable, err error) {
	q := mgr.MakeQuery(kind)
	q.AddFilter(columnName, value)
	return q.ExecuteSingle(nil)
}
