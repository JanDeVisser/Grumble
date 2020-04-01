package grumble

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

type Persistable interface {
	Initialize(Persistable, int) *Key
	SetKind(*Kind)
	Kind() *Kind
	Parent() *Key
	AsKey() *Key
	Id() int
	Self() (Persistable, error)
	SetManager(manager *EntityManager)
	Manager() *EntityManager
	Populated() bool
	SetPopulated()
	SyntheticField(string) (interface{}, bool)
	SetSyntheticField(string, interface{}) bool
	SyntheticFields() map[string]interface{}
}

type GetQueryProcessor interface {
	GetQuery(query *Query) (ret *Query)
}

type GetResultProcessor interface {
	OnGet() (ret Persistable, err error)
}

type ManyQueryProcessor interface {
	ManyQuery(query *Query, values url.Values) (ret *Query)
}

type QueryResultsProcessor interface {
	ProcessQueryResults(results [][]Persistable) (ret [][]Persistable, err error)
}

type PutInterceptor interface {
	OnPut() (err error)
	AfterPut() (err error)
}

type InsertInterceptor interface {
	OnInsert() (err error)
	AfterInsert() (err error)
}

type DeleteInterceptor interface {
	OnDelete() (err error)
}

type LabelBuilder interface {
	Label() string
}

// --------------------------------------------------------------------------

func Label(e Persistable) string {
	if labelBuilder, ok := e.(LabelBuilder); ok {
		return labelBuilder.Label()
	} else {
		k := e.Kind()
		if k.LabelCol == "" {
			return e.AsKey().String()
		} else {
			if val, ok := Field(e, k.LabelCol); ok {
				return fmt.Sprintf("%v", val)
			} else {
				return e.AsKey().String()
			}
		}
	}
}

func Field(e Persistable, fieldName string) (ret interface{}, ok bool) {
	col, ok := e.Kind().Column(fieldName)
	if !ok {
		return
	}
	v := reflect.ValueOf(e).Elem()
	fld := v.FieldByIndex(col.Index)
	if fld.IsValid() && fld.CanInterface() {
		if (fld.Kind() == reflect.Ptr || fld.Kind() == reflect.Interface) && fld.IsNil() {
			ok = false
		} else {
			ret = fld.Interface()
		}
	} else {
		ok = false
	}
	return
}

func SetField(e Persistable, fieldName string, value interface{}) (ok bool) {
	// log.Printf("%s.%s %v %T", e.Kind().Basename(), fieldName, value, value)
	valueVal := reflect.ValueOf(value)
	if !valueVal.IsValid() {
		return
	}
	col, ok := e.Kind().Column(fieldName)
	if !ok {
		return e.SetSyntheticField(fieldName, value)
	}
	v := reflect.ValueOf(e).Elem()
	fld := v.FieldByIndex(col.Index)
	if fld.IsValid() && fld.CanSet() {
		fld.Set(reflect.ValueOf(value))
	}
	return true
}

func SetFields(e Persistable, values map[string]interface{}) {
	v := reflect.ValueOf(e).Elem()
	for name, value := range values {
		col, ok := e.Kind().Column(strings.ToTitle(name))
		if ok {
			fld := v.FieldByIndex(col.Index)
			if fld.IsValid() && fld.CanSet() {
				fld.Set(reflect.ValueOf(value))
			}
		}
	}
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

	copyVal := func(index []int) {
		targetField := targetValue.FieldByIndex(index)
		if targetField.IsValid() && targetField.CanSet() {
			sourceField := sourceValue.FieldByIndex(index)
			targetField.Set(sourceField)
		}
	}
	for _, col := range kind.Columns {
		copyVal(col.Index)
	}
	for _, index := range kind.Transient {
		copyVal(index)
	}
	target.SetManager(src.Manager())
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
	e.Manager().Stash(e)
	ret = e
	return
}

// -- C A C H E -------------------------------------------------------------

type EntityCache struct {
	cache map[*Kind]map[int]Persistable
}

func MakeEntityCache() *EntityCache {
	ret := &EntityCache{cache: make(map[*Kind]map[int]Persistable)}
	return ret
}

func (cache *EntityCache) cacheForKey(e Persistable) (m map[int]Persistable) {
	m, ok := cache.cache[e.Kind()]
	if !ok {
		m = make(map[int]Persistable)
		cache.cache[e.Kind()] = m
	}
	return
}

func (cache *EntityCache) cacheForKind(k *Kind) (m map[int]Persistable) {
	m, ok := cache.cache[k]
	if !ok {
		m = make(map[int]Persistable)
		cache.cache[k] = m
	}
	return
}

func (cache *EntityCache) Put(e Persistable) {
	m := cache.cacheForKey(e)
	m[e.Id()] = e
}

func (cache *EntityCache) Get(k *Kind, id int) (e Persistable) {
	m := cache.cacheForKind(k)
	e = m[id]
	return
}

func (cache *EntityCache) Del(e Persistable) {
	m := cache.cacheForKey(e)
	delete(m, e.Id())
}

func (cache *EntityCache) GetByKey(key *Key) (e Persistable) {
	return cache.Get(key.Kind(), key.Id())
}

func (cache *EntityCache) Has(k *Kind, id int) (ok bool) {
	m := cache.cacheForKind(k)
	_, ok = m[id]
	return
}

// -- P E R S I S T A N C E -------------------------------------------------

type EntityManager struct {
	*PostgreSQLAdapter
	cache *EntityCache
}

func MakeEntityManager() (mgr *EntityManager, err error) {
	mgr = new(EntityManager)
	mgr.PostgreSQLAdapter = GetPostgreSQLAdapter()
	mgr.cache = MakeEntityCache()
	return
}

func (mgr *EntityManager) Make(kind *Kind, parent *Key, id int) (entity Persistable, err error) {
	entity, err = kind.Make(parent, id)
	if entity != nil {
		entity.SetManager(mgr)
	}
	return
}

func (mgr *EntityManager) New(kind *Kind, parent *Key) (entity Persistable, err error) {
	return mgr.Make(kind, parent, 0)
}

func (mgr *EntityManager) Get(kind interface{}, id int) (ret Persistable, err error) {
	k := GetKind(kind)
	if k == nil {
		err = errors.New(fmt.Sprintf("invalid entity kind %T", kind))
		return
	}
	if id <= 0 {
		err = errors.New("cannot Get() entity with ID less than or equal to zero")
		return
	}

	if mgr.cache.Has(k, id) {
		ret = mgr.cache.Get(k, id)
		return
	}

	query := mgr.MakeQuery(kind)
	query.AddCondition(HasId{Id: id})
	query.AddReferenceJoins()

	e, err := mgr.Make(query.Kind, nil, id)
	if err != nil {
		return
	}

	qp, ok := e.(GetQueryProcessor)
	if ok {
		query = qp.GetQuery(query)
	}

	ret, err = query.ExecuteSingle(nil)
	if err != nil {
		return
	}

	if ret != nil {
		grp, ok := ret.(GetResultProcessor)
		if ok {
			ret, err = grp.OnGet()
			if err != nil {
				return
			}
		}
	}
	return
}

func (mgr *EntityManager) Stash(e Persistable) {
	mgr.cache.Put(e)
}

func (mgr *EntityManager) Unstash(e Persistable) {
	mgr.cache.Del(e)
}

func (mgr *EntityManager) Query(kind interface{}, q url.Values) (ret [][]Persistable, err error) {
	query := mgr.MakeQuery(kind)
	e, err := mgr.Make(query.Kind, nil, 0)
	if err != nil {
		return
	}
	op := "="
	if q.Get("_re") != "" {
		op = "~*"
	}
	for _, col := range query.Kind.Columns {
		if q.Get(col.FieldName) != "" {
			refConv, ok := col.Converter.(*ReferenceConverter)
			if ok {
				var id int64
				id, err = strconv.ParseInt(q.Get(col.FieldName), 0, 0)
				if err != nil {
					return
				}
				k, _ := CreateKey(nil, GetKind(refConv.References), int(id))
				query.AddCondition(References{
					Column:     col.FieldName,
					References: k,
				})
			} else {
				query.AddCondition(Predicate{
					Expression: fmt.Sprintf("__alias__.\"%s\"", col.ColumnName),
					Operator:   op,
					Value:      q.Get(col.FieldName),
				})
			}
		}
	}
	if q.Get("_parent") != "" {
		var parent *Key
		parent, err = ParseKey(q.Get("_parent"))
		if err != nil {
			return
		}
		query.AddCondition(&HasParent{Parent: parent})
	}
	if q.Get("_sort") != "" {
		for _, sortorder := range strings.Split(q.Get("_sort"), ";") {
			s := strings.Split(sortorder, ":")
			col := s[0]
			dir := Ascending
			if (len(s) > 1) && (strings.Index(":ASC:DESC:", strings.ToUpper(s[1])) >= 0) {
				dir = SortOrder(s[1])
			}
			query.AddSort(Sort{Column: col, Direction: dir})
		}
	}
	if q.Get("joinparent") != "" {
		pkind := q.Get("joinparent")
		query.AddParentJoin(pkind)
		if q.Get(pkind) != "" {
			var parent Persistable
			var id64 int64
			if id64, err = strconv.ParseInt(q.Get(pkind), 0, 0); err != nil {
				return
			}
			if parent, err = mgr.Get(pkind, int(id64)); err != nil {
				return
			}
			query.AddCondition(&HasParent{Parent: parent.AsKey()})
		}
	} else if query.Kind.ParentKind != nil {
		query.AddParentJoin(query.Kind.ParentKind)
	}
	query.AddReferenceJoins()

	qp, ok := e.(ManyQueryProcessor)
	if ok {
		query = qp.ManyQuery(query, q)
	}

	log.Printf("%s\n", query.SQLText())
	ret, err = query.Execute()
	if err != nil {
		return
	}

	qrp, ok := e.(QueryResultsProcessor)
	if ok {
		ret, err = qrp.ProcessQueryResults(ret)
		if err != nil {
			return
		}
	}

	//log.Printf("%s\n", query.SQLText())
	return
}

func (mgr *EntityManager) Inflate(e Persistable) (err error) {
	SetKind(e)
	ret, err := mgr.Get(e.Kind(), e.Id())
	if ret != nil {
		e.SetKind(ret.Kind())
		e.Initialize(ret.AsKey(), ret.Id())
		if reflect.TypeOf(e) == reflect.TypeOf(ret) {
			_, err = Copy(ret, e)
			if err != nil {
				return
			}
		}
	}
	return
}

var updateEntity = SQLTemplate{Name: "UpdateEntity", SQL: `UPDATE {{.QualifiedTableName}}
	SET {{range $i, $c := .Columns}}{{if not .Formula}}{{if gt $i 0}},{{end}} "{{$c.ColumnName}}" = {{$c.Converter.SQLTextOut .}}{{end}}{{end}}
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
	e.Initialize(nil, id)
	e.SetPopulated()
	return
}

var deleteEntity = SQLTemplate{Name: "DeleteEntity", SQL: `DELETE FROM {{.QualifiedTableName}} WHERE _id = $1`}

func del(e Persistable, conn *sql.DB) (err error) {
	k := e.Kind()
	var sqlText string
	sqlText, err = deleteEntity.Process(k)
	if err != nil {
		return
	}
	values := make([]interface{}, 1)
	values[0] = e.Id()
	if _, err = conn.Exec(sqlText, values...); err != nil {
		return
	}
	return
}

func (mgr *EntityManager) Adopt(e Persistable, children []Persistable) (err error) {
	for _, child := range children {
		child.Initialize(e, child.Id())
		if err = child.Manager().Put(child); err != nil {
			return
		}
	}
	return
}

func (mgr *EntityManager) Put(e Persistable) (err error) {
	SetKind(e)
	return mgr.TX(func(db *sql.DB) (err error) {
		putInterceptor, ok := e.(PutInterceptor)
		if ok {
			if err = putInterceptor.OnPut(); err != nil {
				return
			}
		}
		if e.Id() > 0 {
			if err = update(e, db); err != nil {
				return
			}
		} else {
			insertInterceptor, ok2 := e.(InsertInterceptor)
			if ok2 {
				if err = insertInterceptor.OnInsert(); err != nil {
					return
				}
			}
			if err = insert(e, db); err != nil {
				return
			}
			if ok2 {
				if err = insertInterceptor.AfterInsert(); err != nil {
					return
				}
			}
			mgr.Stash(e)
		}
		if ok {
			if err = putInterceptor.AfterPut(); err != nil {
				return
			}
		}
		return
	})
}

func (mgr *EntityManager) Delete(e Persistable) (err error) {
	return mgr.TX(func(db *sql.DB) (err error) {
		if e.Id() > 0 {
			interceptor, ok := e.(DeleteInterceptor)
			if ok {
				if err = interceptor.OnDelete(); err != nil {
					return
				}
			}
			if err = del(e, db); err == nil {
				mgr.Unstash(e)
			}
		}
		return
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
	query.Manager = mgr
	return query
}

func (mgr *EntityManager) By(kind interface{}, columnName string, value interface{}) (entity Persistable, err error) {
	return mgr.ByColumnAndParent(kind, nil, columnName, value)
}

func (mgr *EntityManager) ByColumnAndParent(kind interface{}, parent *Key, columnName string, value interface{}) (entity Persistable, err error) {
	q := mgr.MakeQuery(kind)
	if parent != nil {
		q.AddCondition(HasParent{Parent: parent})
	}
	q.AddFilter(columnName, value)
	return q.ExecuteSingle(nil)
}

func (mgr *EntityManager) FindOrCreate(kind interface{}, parent *Key, field string, value string) (e Persistable, err error) {
	k := GetKind(kind)
	e, err = mgr.ByColumnAndParent(k, parent, field, value)
	if err != nil {
		err = errors.New(fmt.Sprintf("ByColumnAndParent(%q, %q = %q): %s", parent.String(), field, value, err))
		return
	}
	if e == nil {
		e, err = mgr.Make(k, parent, 0)
		if err != nil {
			return nil, err
		}
		if !SetField(e, field, value) {
			err = errors.New(fmt.Sprintf("could not set field %q on entity of kind %q", field, k.Kind))
			return nil, err
		}
		err = mgr.Put(e)
		if err != nil {
			return nil, err
		}
	}
	return
}

func GetParent(e Persistable) (p Persistable, err error) {
	return e.Manager().Get(e.Parent().Kind(), e.Parent().Ident)
}
