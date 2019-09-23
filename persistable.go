package grumble

import (
	"database/sql"
	"errors"
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
	Populated() bool
	SetPopulated()
	SyntheticField(string) (interface{}, bool)
	SetSyntheticField(string, interface{})
}

// --------------------------------------------------------------------------

func Copy(src, target Persistable) (ret Persistable, err error) {
	if target.Kind() == nil {
		SetKind(target)
	}
	tk := target.Kind()
	sk := src.Kind()
	target.Initialize(src.Parent(), src.Id())
	sourceValue := reflect.ValueOf(src).Elem()
	targetValue := reflect.ValueOf(target).Elem()
	for _, tc := range tk.Columns {
		if sc, ok := sk.Column(tc.ColumnName); ok {
			sourceField := sourceValue.FieldByIndex(sc.Index)
			targetField := targetValue.FieldByIndex(tc.Index)
			targetField.Set(sourceField)
		}
	}
	return target, nil
}

// -- P E R S I S T A N C E -------------------------------------------------

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

func Get(e Persistable, id int) (ret Persistable, err error) {
	if id <= 0 {
		err = errors.New("cannot Get() entity with ID less than or equal to zero")
		return
	}
	query := MakeQuery(e)
	query.AddCondition(HasId{Id: id})
	ret, err = query.ExecuteSingle(e)
	if err != nil {
		return
	}
	return
}

func Inflate(e Persistable) (err error) {
	_, err = Get(e, e.Id())
	return
}

var updateEntity = SQLTemplate{Name: "UpdateEntity", SQL: `UPDATE {{.QualifiedTableName}}
	SET {{range $i, $c := .Columns}}{{if gt $i 0}},{{end}} "{{$c.ColumnName}}" = {{$c.Converter.SQLTextOut .}}{{end}}
	WHERE "_id" = __count__
`}

func update(e Persistable) (err error) {
	if !e.Populated() {
		err = errors.New("cannot update entity. It is not loaded")
	}
	pg := GetPostgreSQLAdapter()
	err = pg.TX(func(conn *sql.DB) (err error) {
		k := e.Kind()
		var sqlText string
		sqlText, err = updateEntity.Process(k)
		if err != nil {
			return
		}
		values := make([]interface{}, 0)
		for _, column := range k.Columns {
			columnValues, err := column.Converter.Value(e, column)
			if err != nil {
				return err
			}
			values = append(values, columnValues...)
		}
		values = append(values, e.Id())
		if _, err = conn.Exec(sqlText, values...); err != nil {
			return
		}
		return
	})
	return
}

var insertEntity = SQLTemplate{Name: "InsertNewEntity", SQL: `INSERT INTO {{.QualifiedTableName}}
	( "_parent"{{range $i, $c := .Columns}}, "{{$c.ColumnName}}"{{end}} )
	VALUES
	( __count__{{range .Columns}}, {{.Converter.SQLTextOut .}}{{end}} )
	RETURNING "_id"
`}

func insert(e Persistable) (err error) {
	pg := GetPostgreSQLAdapter()
	err = pg.TX(func(conn *sql.DB) (err error) {
		k := e.Kind()
		var sqlText string
		sqlText, err = insertEntity.Process(k)
		if err != nil {
			return
		}
		values := make([]interface{}, 0)
		values = append(values, e.AsKey().Parent().Chain())
		for _, column := range k.Columns {
			columnValues, err := column.Converter.Value(e, column)
			if err != nil {
				return err
			}
			values = append(values, columnValues...)
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
	})
	return
}

func Put(e Persistable) (err error) {
	SetKind(e)
	if e.Id() > 0 {
		return update(e)
	} else {
		return insert(e)
	}
}
