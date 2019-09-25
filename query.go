package grumble

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

// --------------------------------------------------------------------------

type Aggregate struct {
	Function string
	Column   string
	Name     string
	Default  string
	Query    *Query
}

func (agg Aggregate) SQLText(alias string) string {
	col := agg.Column
	if col != "*" {
		col = alias + ".\"" + col + "\""
	}
	if agg.Default != "" {
		return fmt.Sprintf("COALESCE(%s(%s), %s) \"%s\"",
			agg.Function, col, agg.Default, agg.Name)
	} else {
		return fmt.Sprintf("%s(%s) \"%s\"",
			agg.Function, col, agg.Name)
	}
}

// --------------------------------------------------------------------------

type Computed struct {
	Formula string
	Name    string
	Query   *Query
}

func (computed Computed) SQLFormula() string {
	return fmt.Sprintf("%s \"%s\"", computed.Formula, computed.Name)
}

// --------------------------------------------------------------------------

type QueryTable struct {
	Kind        *Kind
	WithDerived bool
	Alias       string
	Conditions  CompoundCondition
	GroupBy     bool
	Computed    []Computed
	Aggregates  []Aggregate
	Query       *Query
}

func (table *QueryTable) AddAggregate(agg Aggregate) *QueryTable {
	if table.GroupBy {
		panic("Cannot have aggregates on a grouped Kind")
	}
	if _, ok := table.Kind.ColumnByFieldName(agg.Column); !ok && agg.Column != "*" {
		for _, computed := range table.Computed {
			ok = ok || computed.Name == agg.Column
		}
		if !ok {
			panic(fmt.Sprintf("No column with field name '%s' found", agg.Column))
		}
	}
	agg.Query = table.Query
	if table.Aggregates == nil {
		table.Aggregates = make([]Aggregate, 0)
	}
	table.Aggregates = append(table.Aggregates, agg)
	return table
}

func (table *QueryTable) AddComputedColumn(computed Computed) *QueryTable {
	computed.Query = table.Query
	if table.Computed == nil {
		table.Computed = make([]Computed, 0)
	}
	table.Computed = append(table.Computed, computed)
	return table

}

func (table *QueryTable) AddCondition(cond Condition) *QueryTable {
	table.Conditions.AddCondition(cond)
	return table
}

func (table *QueryTable) IsAggregated() bool {
	return len(table.Aggregates) > 0 && table.Query.IsGrouped() && !table.GroupBy
}

func (table *QueryTable) AddFilter(field string, value interface{}) *QueryTable {
	column, ok := table.Kind.ColumnByFieldName(field)
	if !ok {
		return table
	}
	table.AddCondition(Predicate{
		Expression: fmt.Sprintf("__alias__.\"%s\"", column.ColumnName),
		Operator:   "=",
		Value:      value,
	})
	return table
}

func (table *QueryTable) HasParent(parent Persistable) *QueryTable {
	var pk *Key
	if parent != nil {
		pk = parent.AsKey()
	} else {
		pk = ZeroKey
	}
	table.AddCondition(HasParent{Parent: pk})
	return table
}

// --------------------------------------------------------------------------

type JoinType string

const (
	Inner JoinType = "INNER"
	Outer          = "LEFT"
	Right          = "INNER"
	Left           = "LEFT"
	Cross          = "CROSS"
)

type Join struct {
	QueryTable
	JoinType  JoinType
	FieldName string
}

func (join Join) IsInnerJoin() bool {
	return join.JoinType == Inner || join.JoinType == Cross
}

func (join Join) JoinClause() string {
	if join.JoinType == "" {
		join.JoinType = Inner
	}
	clause := ""
	var ok = join.FieldName == "_parent"
	var column = "\"_parent\"[1]"
	if !ok {
		var c Column
		if join.IsInnerJoin() {
			c, ok = join.Query.Kind.ColumnByFieldName(join.FieldName)
		} else {
			c, ok = join.Kind.ColumnByFieldName(join.FieldName)
		}
		if !ok {
			panic(fmt.Sprintf("Invalid column '%s' in join", join.FieldName))
		}
		column = fmt.Sprintf("%q", c.ColumnName)
	}
	if join.FieldName != "" {
		alias1 := join.Alias
		alias2 := join.Query.Alias
		if !join.IsInnerJoin() {
			alias1 = join.Query.Alias
			alias2 = join.Alias
		}
		clause = fmt.Sprintf("%s JOIN %s ON ( (%s.\"_kind\", %s.\"_id\") = %s.%s)",
			join.JoinType, join.Alias, alias1, alias1, alias2, column)
	}
	if clause == "" {
		panic("Could not render join clause")
	}
	return clause
}

// --------------------------------------------------------------------------

type Query struct {
	pg *PostgreSQLAdapter
	QueryTable
	Joins []Join
}

func MakeQuery(obj interface{}) *Query {
	kind := GetKind(obj)
	if kind == nil {
		panic(fmt.Sprintf("Cannot create query for '%v'", obj))
	}
	query := new(Query)
	query.Kind = kind
	query.Query = query
	return query
}

func (query *Query) AddJoin(join Join) *Query {
	if join.Alias == "" {
		join.Alias = fmt.Sprintf("j%d", len(query.Joins))
	}
	join.Query = query
	query.Joins = append(query.Joins, join)
	return query
}

func (query *Query) AddCount() *Query {
	agg := Aggregate{Function: "COUNT", Column: "*", Name: fmt.Sprintf("%sCount", query.Kind.Basename())}
	query.AddAggregate(agg)
	return query
}

func (query *Query) AddReferenceJoins() *Query {
	for _, col := range query.Kind.Columns {
		if converter, ok := col.Converter.(*ReferenceConverter); ok {
			j := Join{
				QueryTable: QueryTable{
					Kind:        converter.References,
					GroupBy:     false,
					WithDerived: true,
				},
				FieldName:  col.ColumnName,
			}
			query.AddJoin(j)
		}
	}
	return query
}

func (query *Query) GroupedBy() (table *QueryTable) {
	if query.GroupBy {
		return &query.QueryTable
	}
	for _, j := range query.Joins {
		if j.GroupBy {
			table = &j.QueryTable
			return
		}
	}
	return
}

func (query *Query) AggregatedTables() []*QueryTable {
	ret := make([]*QueryTable, 0)
	if query.IsAggregated() {
		ret = append(ret, &query.QueryTable)
	}
	for _, join := range query.Joins {
		if join.IsAggregated() {
			ret = append(ret, &join.QueryTable)
		}
	}
	return ret
}

func (query *Query) TablesWithComputes() []*QueryTable {
	ret := make([]*QueryTable, 0)
	if len(query.Computed) > 0 {
		ret = append(ret, &query.QueryTable)
	}
	for _, join := range query.Joins {
		if len(query.Computed) > 0 {
			ret = append(ret, &join.QueryTable)
		}
	}
	return ret
}

func (query *Query) IsGrouped() bool {
	return query.GroupedBy() != nil
}

var querySQL = SQLTemplate{Name: "Query", SQL: `
WITH 
	{{.Alias}} AS (
		SELECT '{{.Kind.Kind}}' "_kind", "_parent", "_id"
				{{range .Kind.Columns}}, {{.Formula}} {{.Converter.SQLTextIn . "" true}}{{end}} 
				{{range .Computed}}, {{.SQLFormula}}{{end}} 
			FROM {{.Kind.QualifiedTableName}}
		{{if .WithDerived}}{{range .Kind.DerivedKinds}}
		UNION ALL
		SELECT '{{.Kind}}' "_kind", "_parent", "_id"
 				{{range $.Kind.Columns}}, {{.Formula}} {{.Converter.SQLTextIn . "" true}}{{end}} 
				{{range $.Computed}}, {{.SQLFormula}}{{end}} 
			FROM {{.QualifiedTableName}}
		{{end}}{{end}}
		)
	{{range .Joins}},
		{{.Alias}} AS (
			SELECT '{{.Kind.Kind}}' "_kind", "_parent", "_id"
                 	{{range .Kind.Columns}}, {{.Formula}} {{.Converter.SQLTextIn . "" true}}{{end}} 
				    {{range .Computed}}, {{.SQLFormula}}{{end}} 
				FROM {{.Kind.QualifiedTableName}}
			{{$Join := .}}
			{{if .WithDerived}}{{range .Kind.DerivedKinds}}
			UNION ALL
			SELECT '{{.Kind}}' "_kind", "_parent", "_id"
					{{range $Join.Kind.Columns}}, {{.Formula}} {{.Converter.SQLTextIn . "" true}}{{end}} 
					{{range $Join.Computed}}, {{.SQLFormula}}{{end}} 
				FROM {{.QualifiedTableName}}
			{{end}}{{end}}
			{{if gt .Conditions.Size 0}}
				WHERE {{.Conditions.WhereClause .Alias}}
			{{end}}
			)
	{{end}}
SELECT
	{{with .GroupedBy}}{{$JoinAlias := .Alias}}
	{{$JoinAlias}}."_kind", {{$JoinAlias}}."_parent", {{$JoinAlias}}."_id"
		{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $JoinAlias false}}{{end}}
		{{range .Computed}}, {{$JoinAlias}}."{{.Name}}"{{end}}
	{{range $i, $t := $.AggregatedTables}}{{$AggAlias := .Alias}}
	{{range .Aggregates}}, {{.SQLText $AggAlias}}{{end}}
	{{end}}
	{{else}}
	{{$.Alias}}."_kind", {{$.Alias}}."_parent", {{$.Alias}}."_id"
		{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $.Alias false}}{{end}}
		{{range .Computed}}, {{$.Alias}}."{{.Name}}"{{end}}
	{{range .Joins}}{{$JoinAlias := .Alias}}
		, {{$JoinAlias}}."_kind", {{$JoinAlias}}."_parent", {{$JoinAlias}}."_id"
		{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $JoinAlias false}}{{end}}
		{{range .Computed}}, {{$JoinAlias}}."{{.Name}}"{{end}}
	{{end}}
	{{end}}
	FROM {{$.Alias}}
	{{range $.Joins}}{{.JoinClause}}{{end}}
	{{if gt .Conditions.Size 0}}WHERE {{.Conditions.WhereClause .Alias}}{{end}}
	{{with .GroupedBy}}{{$JoinAlias := .Alias}}
	GROUP BY {{$JoinAlias}}."_kind", {{$JoinAlias}}."_parent",{{$JoinAlias}}."_id"{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $JoinAlias false}}{{end}}
	{{end}}
`}

func (query *Query) SQL() (s string, values []interface{}) {
	if query.Alias == "" {
		query.Alias = "k"
	}
	s, err := querySQL.Process(query)
	if err != nil {
		panic(err)
		return
	}
	values = make([]interface{}, 0)
	for _, join := range query.Joins {
		values = join.Conditions.Values(values)
	}
	values = query.Conditions.Values(values)
	return
}

func (query *Query) Execute() (ret [][]Persistable, err error) {
	query.pg = GetPostgreSQLAdapter()
	err = query.pg.TX(func(conn *sql.DB) (err error) {
		sqlText, values := query.SQL()
		rows, err := conn.Query(sqlText, values...)
		if err != nil {
			return
		}
		ret = make([][]Persistable, 0)
		scanners, err := MakeScanners(query)
		if err != nil {
			return
		}
		s, err := scanners.SQLScanners()
		if err != nil {
			return
		}
		for rows.Next() {
			if err = rows.Scan(s...); err != nil {
				return err
			}
			result, err := scanners.Build()
			if err != nil {
				return err
			}
			ret = append(ret, result)
		}
		return
	})
	return
}

func (query *Query) ExecuteSingle(e Persistable) (ret Persistable, err error) {
	results, err := query.Execute()
	switch {
	case err != nil:
		return
	case len(results) == 0:
		return
	case len(results) > 1:
		err = errors.New("call to ExecuteSingle returned more than one result")
		return
	default:
		row := results[0]
		ret = row[0]
		if reflect.TypeOf(e) == reflect.TypeOf(ret) {
			ret, err = Copy(ret, e)
			if err != nil {
				return
			}
		}
		return
	}
}

// -- E N T I T Y S C A N N E R ---------------------------------------------

type KindScanner struct {
	Expects *Kind
	kind    *Kind
}

func (scanner *KindScanner) Scan(value interface{}) (err error) {
	switch k := value.(type) {
	case string:
		if k != "" {
			kind := GetKind(k)
			if kind == nil {
				err = errors.New(fmt.Sprintf("Unknown kind '%s'", k))
				return
			}
			if scanner.Expects != nil {
				if !kind.DerivesFrom(scanner.Expects) {
					err = errors.New(fmt.Sprintf("kind '%s' does not derive from '%s'",
						k, scanner.Expects.Kind))
				}
			}
			scanner.kind = kind
		} else {
			scanner.kind = nil
		}
	case nil:
		scanner.kind = nil
	default:
		err = errors.New(fmt.Sprintf("expected string entity kind, got '%v' (%T)", value, value))
	}
	return
}

type KeyScanner struct {
	Expects *Kind
	Key     *Key
}

func (scanner *KeyScanner) Scan(value interface{}) (err error) {
	chain := ""
	switch c := value.(type) {
	case []uint8:
		chain = string(c)
	case string:
		chain = c
	case nil:
		chain = ""
	default:
		err = errors.New(fmt.Sprintf("expected string key chain, got '%v' (%T)", value, value))
	}
	if err == nil {
		scanner.Key, err = ParseKey(chain)
		if scanner.Expects != nil && scanner.Key.Kind() != nil {
			if !scanner.Key.Kind().DerivesFrom(scanner.Expects) {
				err = errors.New(fmt.Sprintf("kind '%s' does not derive from '%s'",
					scanner.Key.Kind().Kind, scanner.Expects.Kind))
			}
		}
	}
	return
}

type IDScanner struct {
	id int
}

func (scanner *IDScanner) Scan(value interface{}) (err error) {
	switch id := value.(type) {
	case int64:
		scanner.id = int(id)
	case nil:
		scanner.id = 0
	default:
		err = errors.New(fmt.Sprintf("expected string entity parent, got '%q' (%T)", value, value))
	}
	return
}

type EntityScanner struct {
	Query  *Query
	Master *Scanners
	Kind   *Kind
	KindScanner
	ParentScanner KeyScanner
	IDScanner
	SyntheticColumns []string
	values           map[string]interface{}
}

func makeEntityScanner(query *Query, table *QueryTable) *EntityScanner {
	ret := new(EntityScanner)
	ret.Query = query
	ret.Kind = table.Kind
	if len(table.Computed) > 0 {
		if ret.SyntheticColumns == nil {
			ret.SyntheticColumns = make([]string, 0)
		}
		for _, computed := range table.Computed {
			ret.SyntheticColumns = append(ret.SyntheticColumns, computed.Name)
		}
	}
	if table.GroupBy {
		if ret.SyntheticColumns == nil {
			ret.SyntheticColumns = make([]string, 0)
		}
		for _, aggregated := range query.AggregatedTables() {
			for _, agg := range aggregated.Aggregates {
				ret.SyntheticColumns = append(ret.SyntheticColumns, agg.Name)
			}
		}
	}
	ret.values = make(map[string]interface{})
	return ret
}

func (scanner *EntityScanner) SQLScanners(scanners []interface{}) (ret []interface{}, err error) {
	scanners = append(scanners, &scanner.KindScanner, &scanner.ParentScanner, &scanner.IDScanner)
	for _, column := range scanner.Kind.Columns {
		scanners, err = column.Converter.Scanners(column, scanners, scanner.values)
		if err != nil {
			return
		}
	}
	for _, synthetic := range scanner.SyntheticColumns {
		scanners = append(scanners, &BasicScanner{FieldName: synthetic, FieldValues: scanner.values})
	}
	ret = scanners
	return
}

func (scanner *EntityScanner) Build() (entity Persistable, err error) {
	if scanner.kind == nil {
		entity = ZeroKey
		return
	}
	if !scanner.kind.DerivesFrom(scanner.Kind) {
		err = errors.New(fmt.Sprintf("kind '%s' does not derive from '%s'", scanner.kind.Kind, scanner.Kind.Kind))
		return
	}
	entity, err = scanner.kind.Make(scanner.ParentScanner.Key, scanner.IDScanner.id)
	if err != nil {
		return
	}
	entity, err = Populate(entity, scanner.values)
	if err != nil {
		return
	}
	// Clear out values map for next round:
	for name := range scanner.values {
		delete(scanner.values, name)
	}
	return
}

// --------------------------------------------------------------------------

type Scanners struct {
	Query       *Query
	Scanners    []*EntityScanner
	sqlScanners []interface{}
	references  map[string]int
}

func MakeScanners(query *Query) (ret *Scanners, err error) {
	ret = new(Scanners)
	ret.Query = query
	ret.Scanners = make([]*EntityScanner, 1)
	ret.references = make(map[string]int)
	if t := query.GroupedBy(); t != nil {
		ret.Scanners[0] = makeEntityScanner(ret.Query, t)
	} else {
		ret.Scanners[0] = makeEntityScanner(ret.Query, &query.QueryTable)
		for _, join := range query.Joins {
			ret.Scanners = append(ret.Scanners, makeEntityScanner(ret.Query, &join.QueryTable))
			if join.JoinType == Inner {
				ret.references[join.FieldName] = len(ret.Scanners) - 1
			}
		}
	}
	return
}

func (scanners *Scanners) SQLScanners() (ret []interface{}, err error) {
	if scanners.sqlScanners == nil {
		scanners.sqlScanners = make([]interface{}, 0)
		for _, scanner := range scanners.Scanners {
			scanners.sqlScanners, err = scanner.SQLScanners(scanners.sqlScanners)
			if err != nil {
				return
			}
		}
	}
	ret = scanners.sqlScanners
	return
}

func (scanners *Scanners) Build() (ret []Persistable, err error) {
	ret = make([]Persistable, 0)
	for _, scanner := range scanners.Scanners {
		e, err := scanner.Build()
		if err != nil {
			return nil, err
		}
		ret = append(ret, e)
	}
	e := ret[0]
	for columnName, ix := range scanners.references {
		e.SetField(columnName, ret[ix])
	}
	return
}
