package grumble

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// --------------------------------------------------------------------------

type Condition interface {
	WhereClause(string) string
	Values([]interface{}) []interface{}
}

// --------------------------------------------------------------------------

type SimpleCondition struct {
	SQL             string
	ParameterValues []interface{}
}

func (cond SimpleCondition) WhereClause(alias string) string {
	return cond.SQL
}

func (cond SimpleCondition) Values(values []interface{}) []interface{} {
	return append(values, cond.ParameterValues...)
}

// --------------------------------------------------------------------------

type Predicate struct {
	Expression string
	Operator   string
	Value      interface{}
}

func (cond Predicate) WhereClause(alias string) string {
	return fmt.Sprintf("%s %s __count__",
		strings.ReplaceAll(cond.Expression, "__alias__", alias), cond.Operator)
}

func (cond Predicate) Values(values []interface{}) []interface{} {
	return append(values, cond.Value)
}

// --------------------------------------------------------------------------

type HasId struct {
	Id int
}

func (cond HasId) WhereClause(alias string) string {
	return fmt.Sprintf("%s.\"_id\" = __count__", alias)
}

func (cond HasId) Values(values []interface{}) []interface{} {
	return append(values, cond.Id)
}

// --------------------------------------------------------------------------

type HasParent struct {
	Parent *Key
}

func (cond HasParent) WhereClause(alias string) string {
	return fmt.Sprintf("(%s.\"ParentKind\" = __count__ AND %s.\"ParentId\" = __count__)", alias, alias)
}

func (cond HasParent) Values(values []interface{}) []interface{} {
	if cond.Parent == nil {
		cond.Parent = ZeroKey
	}
	k := ""
	if !cond.Parent.IsZero() {
		k = cond.Parent.Kind().Kind
	}
	return append(values, k, cond.Parent.Id())
}

// --------------------------------------------------------------------------

type HasAncestor struct {
	Ancestor *Key
}

func (cond HasAncestor) WhereClause(alias string) string {
	return fmt.Sprintf("%s.\"_parent\" LIKE __count__", alias)
}

func (cond HasAncestor) Values(values []interface{}) []interface{} {
	s := ""
	if cond.Ancestor != nil {
		s = cond.Ancestor.String()
	}
	return append(values, []interface{}{s + "%"})
}

// --------------------------------------------------------------------------

type CompoundCondition struct {
	Conditions []Condition
}

func (compound *CompoundCondition) AddCondition(cond Condition) Condition {
	if compound.Conditions == nil {
		compound.Conditions = make([]Condition, 0)
	}
	compound.Conditions = append(compound.Conditions, cond)
	return compound
}

func (compound CompoundCondition) WhereClause(alias string) string {
	conditions := make([]string, len(compound.Conditions))
	for ix, c := range compound.Conditions {
		conditions[ix] = "(" + c.WhereClause(alias) + ")"
	}
	return strings.Join(conditions, " AND ")
}

func (compound CompoundCondition) Values(values []interface{}) []interface{} {
	for _, c := range compound.Conditions {
		values = c.Values(values)
	}
	return values
}

func (compound CompoundCondition) Size() int {
	return len(compound.Conditions)
}

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
		panic(fmt.Sprintf("No column with field name '%s' found", agg.Column))
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

func (join Join) JoinClause() string {
	if join.JoinType == "" {
		join.JoinType = Inner
	}
	clause := ""
	if join.FieldName != "" {
		if join.JoinType == "INNER" || join.JoinType == "CROSS" {
			column, ok := join.Query.Kind.ColumnByFieldName(join.FieldName)
			if ok {
				clause = fmt.Sprintf("%s JOIN %s ON (%s.\"_id\" = %s.\"%sId\" AND %s.\"_kind\" = %s.\"%sKind\")",
					join.JoinType, join.Alias,
					join.Alias, join.Query.Alias, column.ColumnName,
					join.Alias, join.Query.Alias, column.ColumnName)
			}
		} else {
			column, ok := join.Kind.ColumnByFieldName(join.FieldName)
			if ok {
				clause = fmt.Sprintf("%s JOIN %s ON (%s.\"_id\" = %s.\"%sId\" AND %s.\"_kind\" = %s.\"%sKind\")",
					join.JoinType, join.Alias,
					join.Query.Alias, join.Alias, column.ColumnName,
					join.Query.Alias, join.Alias, column.ColumnName)
			}
		}
	}
	if clause == "" {
		panic("Could not render join clause")
	}
	return clause
}

// --------------------------------------------------------------------------

type Query struct {
	QueryTable
	Joins []Join
}

func MakeQuery(obj interface{}) *Query {
	var kind *Kind
	switch e := obj.(type) {
	case *Key:
		kind = e.kind
	case Key:
		kind = e.kind
	case Persistable:
		if e.Kind() == nil {
			SetKind(e)
		}
		kind = e.Kind()
	case *Kind:
		kind = e
	case Kind:
		kind = &e
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
		SELECT '{{.Kind.Kind}}' "_kind", ("_parent").kind "ParentKind", ("_parent").id "ParentId", "_id"
				{{range .Kind.Columns}}, {{.Converter.SQLTextIn . "" true}}{{end}} 
				{{range .Computed}}, {{.SQLFormula}}{{end}} 
			FROM {{.Kind.QualifiedTableName}}
		{{if .WithDerived}}{{range .Kind.DerivedKinds}}
		UNION ALL
		SELECT '{{.Kind}}' "_kind", ("_parent").kind "ParentKind", ("_parent").id "ParentId", "_id"
 				{{range $.Kind.Columns}}, {{.Converter.SQLTextIn . "" true}}{{end}} 
				{{range $.Computed}}, {{.SQLFormula}}{{end}} 
			FROM {{.QualifiedTableName}}
		{{end}}{{end}}
		)
	{{range .Joins}},
		{{.Alias}} AS (
			SELECT '{{.Kind.Kind}}' "_kind", ("_parent").kind "ParentKind", ("_parent").id "ParentId", "_id"
                 	{{range .Kind.Columns}}, {{.Converter.SQLTextIn . "" true}}{{end}} 
				    {{range .Computed}}, {{.SQLFormula}}{{end}} 
				FROM {{.Kind.QualifiedTableName}}
			{{$Join := .}}
			{{if .WithDerived}}{{range .Kind.DerivedKinds}}
			UNION ALL
			SELECT '{{.Kind}}' "_kind", ("_parent").kind "ParentKind", ("_parent").id "ParentId", "_id"
					{{range $Join.Kind.Columns}}, {{.Converter.SQLTextIn . "" true}}{{end}} 
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
	{{$JoinAlias}}."_kind", {{$JoinAlias}}."ParentKind", {{$JoinAlias}}."ParentId", {{$JoinAlias}}."_id"
		{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $JoinAlias false}}{{end}}
		{{range .Computed}}, "{{.Name}}"{{end}}
	{{range $i, $t := $.AggregatedTables}}{{$AggAlias := .Alias}}
	{{range .Aggregates}}, {{.SQLText $AggAlias}}{{end}}
	{{end}}
	{{else}}
	{{$.Alias}}."_kind", {{$.Alias}}."ParentKind", {{$.Alias}}."ParentId", {{$.Alias}}."_id"
		{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $.Alias false}}{{end}}
		{{range .Computed}}, "{{.Name}}"{{end}}
	{{range .Joins}}{{$JoinAlias := .Alias}}
		, {{$JoinAlias}}."_kind", {{$JoinAlias}}."ParentKind", {{$JoinAlias}}."ParentId", {{$JoinAlias}}."_id"
		{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $JoinAlias false}}{{end}}
		{{range .Computed}}, "{{.Name}}"{{end}}
	{{end}}
	{{end}}
	FROM {{$.Alias}}
	{{range .Joins}}{{.JoinClause}}{{end}}
	{{if gt .Conditions.Size 0}}WHERE {{.Conditions.WhereClause .Alias}}{{end}}
	{{with .GroupedBy}}{{$JoinAlias := .Alias}}
	GROUP BY {{$JoinAlias}}."_kind", {{$JoinAlias}}."ParentKind", {{$JoinAlias}}."ParentId", {{$JoinAlias}}."_id"{{range .Kind.Columns}}, {{.Converter.SQLTextIn . $JoinAlias false}}{{end}}
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
	pg := GetPostgreSQLAdapter()
	err = pg.TX(func(conn *sql.DB) (err error) {
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
			ret, err = ret.Kind().Copy(ret, e)
			if err != nil {
				return
			}
		}
		return
	}
}

// -- E N T I T Y S C A N N E R ---------------------------------------------

type KindScanner struct {
	kind *Kind
}

func (scanner *KindScanner) Scan(value interface{}) (err error) {
	switch k := value.(type) {
	case string:
		if k != "" {
			kind := GetKindForKind(k)
			if kind == nil {
				err = errors.New(fmt.Sprintf("Unknown kind '%s'", k))
				return
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

type ParentScanner struct {
	KindScanner
	IDScanner
	Parent *Key
	count  int
}

func (scanner *ParentScanner) Scan(value interface{}) (err error) {
	switch scanner.count {
	case 0:
		if err = scanner.KindScanner.Scan(value); err != nil {
			return
		}
	case 1:
		if err = scanner.IDScanner.Scan(value); err != nil {
			return
		}
		if scanner.kind == nil {
			scanner.Parent = ZeroKey
		} else {
			if scanner.Parent, err = CreateKey(nil, scanner.kind, scanner.id); err != nil {
				return
			}

		}
		scanner.count = -1
	}
	scanner.count++
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
	ParentScanner
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
	scanners = append(scanners, &scanner.KindScanner, &scanner.ParentScanner, &scanner.ParentScanner, &scanner.IDScanner)
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
	entity, err = scanner.kind.Make(scanner.Parent, scanner.IDScanner.id)
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
}

func MakeScanners(query *Query) (ret *Scanners, err error) {
	ret = new(Scanners)
	ret.Query = query
	ret.Scanners = make([]*EntityScanner, 1)
	if t := query.GroupedBy(); t != nil {
		ret.Scanners[0] = makeEntityScanner(ret.Query, t)
	} else {
		ret.Scanners[0] = makeEntityScanner(ret.Query, &query.QueryTable)
		for _, join := range query.Joins {
			ret.Scanners = append(ret.Scanners, makeEntityScanner(ret.Query, &join.QueryTable))
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
	return
}
