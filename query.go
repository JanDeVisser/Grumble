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

type JoinDirection string

const (
	Referring  JoinDirection = "out"
	Out                      = "out"
	ReferredBy               = "in"
	In                       = "in"
)

type JoinType string

const (
	Inner JoinType = "INNER"
	Outer          = "LEFT"
	Full           = "LEFT"
	Right          = "INNER"
	Left           = "LEFT"
	Cross          = "CROSS"
)

type Join struct {
	QueryTable
	Direction JoinDirection
	JoinType  JoinType
	FieldName string
	Reference bool
}

func (join Join) IsInnerJoin() bool {
	return join.JoinType == Inner || join.JoinType == Cross
}

func (join Join) JoinClause() string {
	if join.Direction == "" {
		join.Direction = Referring
	}
	if join.JoinType == "" {
		join.JoinType = Inner
	}
	clause := ""
	var ok = join.FieldName == "_parent"
	var column = "\"_parent\"[1]"
	if !ok {
		var c Column
		if join.Direction == Referring {
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
		var alias1, alias2 string
		if join.Direction == Referring {
			alias1 = join.Alias
			alias2 = join.Query.Alias
		} else {
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
	mgr *EntityManager
	QueryTable
	Joins []Join
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
				FieldName: col.ColumnName,
				Reference: true,
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
	err = query.mgr.TX(func(conn *sql.DB) (err error) {
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
		e.SetKind(ret.Kind())
		e.Initialize(ret.AsKey(), ret.Id())
		if reflect.TypeOf(e) == reflect.TypeOf(ret) {
			ret, err = Copy(ret, e)
			if err != nil {
				return
			}
		}
		return
	}
}
