package grumble

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Key struct {
	kind      *Kind
	parent    *Key
	id        int
	populated bool
	synthetic map[string]interface{}
}

// -- F A C T O R Y  M E T H O D S ------------------------------------------

var ZeroKey = &Key{parent: nil, kind: nil, id: 0}

func CreateKey(parent *Key, kind *Kind, id int) (*Key, error) {
	ret := new(Key)
	if parent == nil {
		parent = ZeroKey
	}
	ret.parent = parent
	ret.kind = kind
	ret.id = id
	return ret, nil
}

func ParseKey(key string) (k *Key, err error) {
	if key == "" {
		k = ZeroKey
		return
	}
	var parent *Key
	var local string
	if lastSlash := strings.LastIndex(key, "/"); lastSlash > 0 {
		parent, err = ParseKey(key[:lastSlash])
		if err != nil {
			return
		}
		local = key[lastSlash+1:]
	} else {
		parent = ZeroKey
		local = key
	}
	if strings.Index(local, ":") < 0 {
		return nil, errors.New(fmt.Sprintf("format error in key '%s': local part has no ':'", key))
	}
	kindId := strings.Split(local, ":")
	kind := GetKindForKind(kindId[0])
	if kind == nil {
		err = errors.New(fmt.Sprintf("parsing key '%s': kind '%s' does not exist", key, kindId[0]))
		return
	}
	id, err := strconv.ParseInt(kindId[1], 0, 0)
	if err != nil {
		err = errors.New(fmt.Sprintf("parsing key '%s': ID '%s' is not an integer", key, kindId[1]))
		return
	}
	return CreateKey(parent, kind, int(id))
}

// -- P E R S I S T A B L E  I M P L E M E N T A T I O N --------------------

func (key *Key) Initialize(parent *Key, id int) *Key {
	if parent == nil {
		parent = ZeroKey
	}
	key.parent = parent
	key.id = id
	return key
}

func (key *Key) SetKind(k *Kind) {
	//if key.kind != nil && key.kind.Kind != k.Kind {
	//	panic(fmt.Sprintf("Attempt to change entity Kind from '%s' to '%s'", key.kind.Kind, k.Kind))
	//}
	key.kind = k
}

func (key *Key) Kind() *Kind {
	if key.kind == nil {
		key.kind = GetKind(key)
	}
	return key.kind
}

func (key *Key) Parent() *Key {
	return key.parent
}

func (key *Key) AsKey() *Key {
	return key
}

func (key *Key) IsZero() bool {
	return key.kind == nil
}

func (key *Key) Id() int {
	return key.id
}

func (key *Key) Self() (ret Persistable, err error) {
	ret, err = key.Kind().Make(key.parent, key.Id())
	if err != nil {
		return
	}
	return Get(ret, key.Id())
}

func (key *Key) SyntheticField(name string) (ret interface{}, ok bool) {
	ret, ok = key.synthetic[name]
	return
}

func (key *Key) SetSyntheticField(name string, value interface{}) {
	if key.synthetic == nil {
		key.synthetic = make(map[string]interface{})
	}
	key.synthetic[name] = value
}

func (key *Key) Populated() bool {
	return key.populated
}

func (key *Key) SetPopulated() {
	key.populated = true
}

// -- E N D  I M P L E M E N T A T I O N ------------------------------------

func (key Key) String() string {
	if key.IsZero() {
		return ""
	}
	local := fmt.Sprintf("%s:%d", key.Kind().Name(), key.id)
	switch {
	case key.parent.IsZero():
		return local
	default:
		return fmt.Sprintf("%s%s/", key.parent, local)
	}
}

func (key *Key) Field(fieldName string) interface{} {
	v := reflect.ValueOf(key)
	if fld := v.FieldByName(fieldName); fld.IsValid() && fld.CanInterface() {
		return fld.Interface()
	} else {
		return nil
	}
}

func (key *Key) Label() string {
	k := key.Kind()
	if k.LabelCol == "" {
		return key.String()
	} else {
		return fmt.Sprintf("%v", key.Field(k.LabelCol))
	}
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
	( (__count__, __count__){{range .Columns}}, {{.Converter.SQLTextOut .}}{{end}} )
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
		if e.Parent().IsZero() {
			values = append(values, "", 0)
		} else {
			values = append(values, e.Parent().Kind().Kind, e.Parent().Id())
		}
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
