package grumble

import (
	"fmt"
	"strings"
)

// --------------------------------------------------------------------------

type Condition interface {
	WhereClause(*Query, bool) string
	Values([]interface{}) []interface{}
}

// --------------------------------------------------------------------------

type SimpleCondition struct {
	SQL             string
	ParameterValues []interface{}
}

func (cond *SimpleCondition) WhereClause(query *Query, queryConstraint bool) string {
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	return strings.ReplaceAll(cond.SQL, "__alias__.", alias)
}

func (cond *SimpleCondition) Values(values []interface{}) []interface{} {
	return append(values, cond.ParameterValues...)
}

// --------------------------------------------------------------------------

type Predicate struct {
	Expression string
	Operator   string
	Value      interface{}
}

func (cond *Predicate) WhereClause(query *Query, queryConstraint bool) string {
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	return fmt.Sprintf("%s %s __count__",
		strings.ReplaceAll(cond.Expression, "__alias__.", alias), cond.Operator)
}

func (cond *Predicate) Values(values []interface{}) []interface{} {
	ret := append(values, cond.Value)
	return ret
}

// --------------------------------------------------------------------------

type HasId struct {
	Id int
}

func (cond *HasId) WhereClause(query *Query, queryConstraint bool) string {
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	return fmt.Sprintf("%s\"_id\" = __count__", alias)
}

func (cond *HasId) Values(values []interface{}) []interface{} {
	return append(values, cond.Id)
}

// --------------------------------------------------------------------------

type HasMaxValue struct {
	Column string
}

func (cond *HasMaxValue) WhereClause(query *Query, queryConstraint bool) string {
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	return fmt.Sprintf("%s%q = (SELECT MAX(%q) FROM %s %s)",
		alias, cond.Column, cond.Column, query.Kind.QualifiedTableName(), query.SelectWhereClause(false))
}

func (cond *HasMaxValue) Values(values []interface{}) []interface{} {
	return values
}

// --------------------------------------------------------------------------

type HasMinValue struct {
	Column string
}

func (cond *HasMinValue) WhereClause(query *Query, queryConstraint bool) string {
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	return fmt.Sprintf("%s%q = (SELECT MIN(%q) FROM %s)", alias, cond.Column, cond.Column, query.Kind.QualifiedTableName())
}

func (cond *HasMinValue) Values(values []interface{}) []interface{} {
	return values
}

// --------------------------------------------------------------------------

type HasParent struct {
	Parent *Key
}

func (cond *HasParent) WhereClause(query *Query, queryConstraint bool) string {
	if cond.Parent == nil {
		cond.Parent = ZeroKey
	}
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	if cond.Parent.IsZero() {
		return fmt.Sprintf("cardinality(%s\"_parent\") = 0", alias)
	} else {
		return fmt.Sprintf("%s\"_parent\"[1] = __count__::%s.\"Reference\"", alias, query.Manager.Schema)
	}
}

func (cond *HasParent) Values(values []interface{}) (ret []interface{}) {
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

func (cond *HasAncestor) WhereClause(query *Query, queryConstraint bool) string {
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	if cond.Ancestor != nil && !cond.Ancestor.IsZero() {
		return fmt.Sprintf("__count__::%s.\"Reference\" = ANY(%s\"_parent\")", query.Manager.Schema, alias)
	} else {
		return "1 = 1"
	}
}

func (cond *HasAncestor) Values(values []interface{}) (ret []interface{}) {
	ret = values
	if cond.Ancestor != nil && !cond.Ancestor.IsZero() {
		ret = append(values, cond.Ancestor.String())
	}
	return
}

// --------------------------------------------------------------------------

type IsRoot struct {
}

func (cond *IsRoot) WhereClause(query *Query, queryConstraint bool) string {
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	return fmt.Sprintf("cardinality(%s\"_parent\") = 0", alias)
}

func (cond *IsRoot) Values(values []interface{}) (ret []interface{}) {
	ret = values
	return
}

// --------------------------------------------------------------------------

type References struct {
	Column     string
	References interface{}
	references []*Key
	hasZero    bool
	Invert     bool
}

func (cond *References) where(schema string, alias string) string {
	params := ""
	if len(cond.references) > 0 {
		params = strings.Repeat(
			fmt.Sprintf("__count__::%s.\"Reference\", ", schema),
			len(cond.references))
		params = params[0 : len(params)-2]
	}

	switch {
	case cond.references == nil && !cond.Invert:
		return fmt.Sprintf("%s%q IS NULL", alias, cond.Column)
	case cond.references == nil && cond.Invert:
		return fmt.Sprintf("%s%q IS NOT NULL", alias, cond.Column)
	case !cond.hasZero && !cond.Invert:
		return fmt.Sprintf("%s%q IN ( %s )", alias, cond.Column, params)
	case !cond.hasZero && cond.Invert:
		return fmt.Sprintf("(%s%q NOT IN ( %s ) OR %s%q IS NULL)", alias, cond.Column, params, alias, cond.Column)
	case cond.hasZero && !cond.Invert:
		return fmt.Sprintf("(%s%q IN ( %s ) OR %s%q IS NULL)",
			alias, cond.Column, params, alias, cond.Column)
	case cond.hasZero && cond.Invert:
		return fmt.Sprintf("(%s%q NOT IN ( %s ) AND %s%q IS NOT NULL)",
			alias, cond.Column, params, alias, cond.Column)
	}
	panic("INCONSISTENT References CONDITION STATE!")
}

func (cond *References) WhereClause(query *Query, queryConstraint bool) string {
	if cond.References == nil {
		cond.References = ZeroKey
	}
	switch ref := cond.References.(type) {
	case *Key:
		if ref.IsZero() || ref == nil {
			cond.references = nil
			cond.hasZero = true
		} else {
			cond.references = make([]*Key, 1)
			cond.references[0] = ref
			cond.hasZero = false
		}
	case Persistable:
		if ref != nil {
			cond.references = make([]*Key, 1)
			cond.references[0] = ref.AsKey()
			cond.hasZero = false
		} else {
			cond.references = nil
		}
	case []*Key:
		cond.hasZero = false
		cond.references = make([]*Key, 0)
		for ix := range ref {
			if ref[ix].IsZero() || ref[ix] == nil {
				cond.hasZero = true
			} else {
				cond.references = append(cond.references, ref[ix])
			}
		}
		if len(cond.references) == 0 {
			cond.references = nil
		}
	case []Persistable:
		if len(ref) > 0 {
			cond.hasZero = false
			cond.references = make([]*Key, 0)
			for ix := range ref {
				if ref[ix] == nil {
					cond.hasZero = true
				} else {
					cond.references = append(cond.references, ref[ix].AsKey())
				}
			}
		} else {
			cond.references = nil
		}
	default:
		panic(fmt.Sprintf("Can't do References condition using %v (%T)", ref, ref))
	}
	if cond.references != nil && len(cond.references) == 0 {
		cond.references = nil
	}
	alias := ""
	if queryConstraint {
		alias = query.Alias + "."
	}
	return cond.where(query.Manager.Schema, alias)
}

func (cond *References) Values(values []interface{}) (ret []interface{}) {
	ret = values
	if cond.references != nil {
		for ix := range cond.references {
			ret = append(ret, cond.references[ix].String())
		}
	}
	return
}

// --------------------------------------------------------------------------

type CompoundCondition struct {
	Conditions []Condition
	Operand    string
}

func (compound *CompoundCondition) AddCondition(cond Condition) Condition {
	if compound.Conditions == nil {
		compound.Conditions = make([]Condition, 0)
	}
	compound.Conditions = append(compound.Conditions, cond)
	return compound
}

func (compound *CompoundCondition) WhereClause(query *Query, queryConstraint bool) string {
	conditions := make([]string, len(compound.Conditions))
	for ix, c := range compound.Conditions {
		conditions[ix] = "(" + c.WhereClause(query, queryConstraint) + ")"
	}
	if compound.Operand == "" {
		compound.Operand = "AND"
	}
	return strings.Join(conditions, fmt.Sprintf(" %s ", compound.Operand))
}

func (compound *CompoundCondition) Values(values []interface{}) []interface{} {
	for _, c := range compound.Conditions {
		values = c.Values(values)
	}
	return values
}

func (compound *CompoundCondition) Size() int {
	return len(compound.Conditions)
}
