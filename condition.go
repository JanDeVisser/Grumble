package grumble

import (
	"fmt"
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
	if cond.Parent == nil {
		cond.Parent = ZeroKey
	}
	if cond.Parent.IsZero() {
		return fmt.Sprintf("cardinality(%s.\"_parent\") = 0", alias)
	} else {
		pg := GetPostgreSQLAdapter()
		return fmt.Sprintf("%s.\"_parent\"[1] = __count__::%s.\"Reference\"", alias, pg.Schema)
	}
}

func (cond HasParent) Values(values []interface{}) (ret []interface{}) {
	if cond.Parent == nil {
		cond.Parent = ZeroKey
	}
	ret = values
	if !cond.Parent.IsZero() {
		ret = append(values, cond.Parent.String())
	}
	return
}

// --------------------------------------------------------------------------

type HasAncestor struct {
	Ancestor *Key
}

func (cond HasAncestor) WhereClause(alias string) string {
	if cond.Ancestor != nil && !cond.Ancestor.IsZero() {
		pg := GetPostgreSQLAdapter()
		return fmt.Sprintf("__count__::%s.\"Reference\" = ANY(%s.\"_parent\")", pg.Schema, alias)
	} else {
		return "1 = 1"
	}
}

func (cond HasAncestor) Values(values []interface{}) (ret []interface{}) {
	ret = values
	if cond.Ancestor != nil && !cond.Ancestor.IsZero() {
		ret = append(values, cond.Ancestor.String())
	}
	return
}

// --------------------------------------------------------------------------

type References struct {
	Column     string
	References *Key
}

func (cond References) WhereClause(alias string) string {
	if cond.References == nil {
		cond.References = ZeroKey
	}
	if cond.References.IsZero() {
		return fmt.Sprintf("%s.%q IS NULL", alias, cond.Column)
	} else {
		pg := GetPostgreSQLAdapter()
		return fmt.Sprintf("%s.%q = __count__::%s.\"Reference\"", alias, cond.Column, pg.Schema)
	}
}

func (cond References) Values(values []interface{}) (ret []interface{}) {
	if cond.References == nil {
		cond.References = ZeroKey
	}
	ret = values
	if !cond.References.IsZero() {
		ret = append(values, cond.References.String())
	}
	return
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
