/*
 * This file is part of Finn.
 *
 * Copyright (c) 2019 Jan de Visser <jan@finiandarcy.com>
 *
 * Finn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Finn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Finn.  If not, see <https://www.gnu.org/licenses/>.
 */

package grumble

import (
	"errors"
	"fmt"
)

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

func makeEntityScanner(query *Query, table *QueryTable, subQueries []SubQuery, extraComputed []Computed) *EntityScanner {
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
	if len(subQueries) > 0 {
		if ret.SyntheticColumns == nil {
			ret.SyntheticColumns = make([]string, 0)
		}
		for _, sq := range subQueries {
			for _, ss := range sq.SubSelects {
				ret.SyntheticColumns = append(ret.SyntheticColumns, ss.Name)
			}
		}
	}
	if len(extraComputed) > 0 {
		if ret.SyntheticColumns == nil {
			ret.SyntheticColumns = make([]string, 0)
		}
		for _, computed := range extraComputed {
			ret.SyntheticColumns = append(ret.SyntheticColumns, computed.Name)
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
	entity, err = scanner.Query.Manager.Make(scanner.kind, scanner.ParentScanner.Key, scanner.IDScanner.id)
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
		ret.Scanners[0] = makeEntityScanner(ret.Query, t, query.SubQueries, query.GlobalComputed)
	} else {
		ret.Scanners[0] = makeEntityScanner(ret.Query, &query.QueryTable, query.SubQueries, query.GlobalComputed)
		for _, join := range query.Joins {
			ret.Scanners = append(ret.Scanners, makeEntityScanner(ret.Query, &join.QueryTable, nil, nil))
			if join.Reference {
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
	kind := e.Kind()
	for columnName, ix := range scanners.references {
		ref := ret[ix]
		if ref.AsKey().IsZero() {
			ref = nil
		} else {
			col, ok := kind.Column(columnName)
			if ok {
				k := col.Converter.(*ReferenceConverter).References
				SetField(e, columnName, CastTo(ref, k))
			}
		}
	}
	return
}
