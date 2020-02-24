package grumble

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
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
	Alias   string
	Name    string
	Query   *Query
}

func (computed Computed) SQLFormula() string {
	alias := ""
	if computed.Alias != "" {
		alias = computed.Alias + "."
	}
	return fmt.Sprintf("%s %s%q", computed.Formula, alias, computed.Name)
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

func (table *QueryTable) WhereClause(queryConstraint bool) string {
	if table.Conditions.Size() > 0 {
		return "WHERE " + table.Conditions.WhereClause(table.Query, queryConstraint)
	} else {
		return ""
	}
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
	Direction    JoinDirection
	JoinType     JoinType
	FieldName    string
	Reference    bool
	IsSuppressed bool
}

func (join Join) IsInnerJoin() bool {
	return join.JoinType == Inner || join.JoinType == Cross
}

func (join Join) JoinClause() string {
	if join.IsSuppressed {
		return ""
	}
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

type SubQuery struct {
	QueryTable
	Where      string
	SubSelects []Computed
}

func (sq *SubQuery) AddSubSelect(computed Computed) *SubQuery {
	if sq.SubSelects == nil {
		sq.SubSelects = make([]Computed, 0)
	}
	sq.SubSelects = append(sq.SubSelects, computed)
	return sq
}

func (sq *SubQuery) SQLText() string {
	if len(sq.SubSelects) == 0 {
		return ""
	}
	columns := make([]string, len(sq.SubSelects))
	for ix, subSelect := range sq.SubSelects {
		where := ""
		if sq.Where != "" {
			where = " WHERE " + sq.Where
		}
		columns[ix] = fmt.Sprintf("(SELECT %s FROM %s%s) %q", subSelect.Formula, sq.Alias, where, subSelect.Name)
	}
	return ", " + strings.Join(columns, ", ")
}

// --------------------------------------------------------------------------

type SortOrder string

const (
	Ascending  SortOrder = "ASC"
	Descending SortOrder = "DESC"
)

type Sort struct {
	Alias     string
	Column    string
	Direction SortOrder
	Query     *Query
}

func (sort *Sort) SQLText() string {
	if sort.Alias == "" {
		sort.Alias = sort.Query.Alias
	}
	if sort.Direction == "" {
		sort.Direction = Ascending
	}
	return fmt.Sprintf("%s.%q %s", sort.Alias, sort.Column, sort.Direction)
}

// --------------------------------------------------------------------------

type Query struct {
	QueryTable
	Manager         *EntityManager
	Joins           []Join
	SubQueries      []SubQuery
	GlobalComputed  []Computed
	QueryConditions CompoundCondition
	Sorting         []Sort
}

func (query *Query) AddQueryCondition(cond Condition) *Query {
	query.QueryConditions.AddCondition(cond)
	return query
}

func (query *Query) AddJoin(join Join) *Query {
	if query.Joins == nil {
		query.Joins = make([]Join, 0)
	}
	if join.Alias == "" {
		join.Alias = fmt.Sprintf("j%d", len(query.Joins))
	}
	join.Query = query
	query.Joins = append(query.Joins, join)
	return query
}

func (query *Query) RemoveJoin(alias string) {
	for ix, j := range query.Joins {
		if j.Alias == alias {
			if ix < len(query.Joins)-1 {
				query.Joins[ix] = query.Joins[len(query.Joins)-1]
			}
			query.Joins = query.Joins[:len(query.Joins)-1]
			break
		}
	}
}

func (query *Query) AddParentJoin(kind interface{}) *Query {
	k := GetKind(kind)
	if k == nil {
		return query
	}
	j := Join{
		QueryTable: QueryTable{
			Kind:        k,
			Alias:       "parent",
			GroupBy:     false,
			WithDerived: true,
		},
		FieldName: "_parent",
		JoinType:  Outer,
		Direction: Out,
		Reference: true,
	}
	return query.AddJoin(j)
}

func (query *Query) AddSubQuery(sq SubQuery) *Query {
	if query.SubQueries == nil {
		query.SubQueries = make([]SubQuery, 0)
	}
	sq.Query = query
	if sq.Alias == "" {
		sq.Alias = fmt.Sprintf("sq%d", len(query.SubQueries))
	}
	query.SubQueries = append(query.SubQueries, sq)
	return query
}

func (query *Query) AddGlobalComputedColumn(computed Computed) *Query {
	computed.Query = query.Query
	if query.GlobalComputed == nil {
		query.GlobalComputed = make([]Computed, 0)
	}
	query.GlobalComputed = append(query.GlobalComputed, computed)
	return query
}

func (query *Query) AddSort(sort Sort) *Query {
	if query.Sorting == nil {
		query.Sorting = make([]Sort, 0)
	}
	sort.Query = query
	query.Sorting = append(query.Sorting, sort)
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
				JoinType:  Outer,
				Direction: Out,
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

func (query *Query) SelectWhereClause(queryConstraint bool) string {
	if query.QueryConditions.Size() > 0 {
		return "WHERE " + query.QueryConditions.WhereClause(query, queryConstraint)
	} else {
		return ""
	}
}

var querySQL = SQLTemplate{Name: "Query", SQL: `
{{define "WithTable"}}{{$Current := .}}
	{{.Alias}} AS (
		SELECT '{{.Kind.Kind}}' "_kind", "_parent", "_id"
				{{range .Kind.Columns}}, {{.Formula}} {{.Converter.SQLTextIn . "" true}}{{end}} 
				{{range .Computed}}, {{.SQLFormula}}{{end}} 
			FROM {{.Kind.QualifiedTableName}}
		    {{.WhereClause false}}
		{{if .WithDerived}}{{range .Kind.DerivedKinds}}
		UNION ALL
		SELECT '{{.Kind}}' "_kind", "_parent", "_id"
 				{{range $Current.Kind.Columns}}, {{.Formula}} {{.Converter.SQLTextIn . "" true}}{{end}} 
				{{range $Current.Computed}}, {{.SQLFormula}}{{end}} 
			FROM {{.QualifiedTableName}}
		    {{$Current.WhereClause false}}
		{{end}}{{end}}
	)
{{end}}
{{define "SelectFrom"}}
	{{$Alias := .Alias}}{{$Alias}}."_kind", {{$Alias}}."_parent", {{$Alias}}."_id"
	{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $Alias false}}{{end}}
	{{range .Computed}}, {{$Alias}}."{{.Name}}"{{end}}
{{end}}
WITH 
	{{template "WithTable" .}}
	{{range .Joins}},
		{{template "WithTable" .}}
	{{end}}
	{{range .SubQueries}},
		{{template "WithTable" .}}
	{{end}}
SELECT
	{{with .GroupedBy}}
		{{template "SelectFrom" .}}
		{{range $i, $t := $.AggregatedTables}}{{$AggAlias := .Alias}}
			{{range .Aggregates}}, {{.SQLText $AggAlias}}{{end}}
		{{end}}
		{{range $.SubQueries}}
		{{.SQLText}}{{end}}
		{{range $.GlobalComputed}}, {{.SQLFormula}}{{end}}
	{{else}}
		{{template "SelectFrom" .}}
		{{range $.SubQueries}}
		{{.SQLText}}{{end}}
		{{range $.GlobalComputed}}, {{.SQLFormula}}{{end}}
		{{range .Joins}}, {{template "SelectFrom" .}}{{end}}
	{{end}}
	FROM {{$.Alias}}
	{{range $.Joins}}
	{{.JoinClause}} {{end}}
	{{.SelectWhereClause true}}
	{{with .GroupedBy}}{{$JoinAlias := .Alias}}
	GROUP BY {{$JoinAlias}}."_kind", {{$JoinAlias}}."_parent",{{$JoinAlias}}."_id"{{range .Kind.Columns}}, 
			 {{.Converter.SQLTextIn . $JoinAlias false}}{{end}}
	{{end}}
	ORDER BY{{range .Sorting}} {{.SQLText}},{{end}} {{$.Alias}}."_id" ASC 
`}

func (query *Query) SQLText() (s string) {
	if query.Alias == "" {
		query.Alias = "k"
	}
	for _, computed := range query.GlobalComputed {
		computed.Alias = query.Alias
	}
	s, err := querySQL.Process(query)
	if err != nil {
		panic(err)
		return
	}
	return
}

func (query *Query) SQL() (s string, values []interface{}) {
	s = query.SQLText()
	values = make([]interface{}, 0)
	values = query.Conditions.Values(values)
	if query.WithDerived {
		for _, _ = range query.Kind.DerivedKinds() {
			values = query.Conditions.Values(values)
		}
	}
	for _, join := range query.Joins {
		values = join.Conditions.Values(values)
		if join.WithDerived {
			for _, _ = range join.Kind.DerivedKinds() {
				values = join.Conditions.Values(values)
			}
		}
	}
	for _, sq := range query.SubQueries {
		values = sq.Conditions.Values(values)
	}
	values = query.QueryConditions.Values(values)
	return
}

func (query *Query) Execute() (ret [][]Persistable, err error) {
	err = query.Manager.TX(func(conn *sql.DB) (err error) {
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
		if e != nil {
			e.SetKind(ret.Kind())
			e.Initialize(ret.AsKey(), ret.Id())
			if reflect.TypeOf(e) == reflect.TypeOf(ret) {
				ret, err = Copy(ret, e)
				if err != nil {
					return
				}
			}
		}
		return
	}
}
