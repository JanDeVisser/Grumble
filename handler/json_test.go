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

package handler

import (
	"errors"
	"github.com/JanDeVisser/grumble"
	"testing"
)

type X struct {
	Foo int
	Bar string
}

func (x X) Marshal(json interface{}) (err error) {
	switch m := json.(type) {
	case map[string]interface{}:
		m["Frob"] = false
	default:
		err = errors.New("marshalled object is not a map")
	}
	return
}

type Person struct {
	grumble.Key
	FirstName string
	LastName  string
	Age       int
}

func (p *Person) Marshal(json interface{}) (err error) {
	var parent grumble.Persistable
	var parentJSON interface{}
	if p.Parent() != nil && p.Parent() != grumble.ZeroKey {
		parent, err = p.Manager().Get(p.Parent().Kind(), p.Parent().Ident)
		if err != nil {
			return
		}
		parentJSON, err = MarshalPersistableToMap(parent)
		if err != nil {
			return
		}
	}
	switch m := json.(type) {
	case map[string]interface{}:
		m["_parent"] = parentJSON
	default:
		err = errors.New("marshalled object is not a map")
	}
	return
}

func TestMarshal(t *testing.T) {
	x := X{Foo: 42, Bar: "quux"}
	if m, err := Marshal(x); err != nil {
		t.Errorf("Error marshaling struct: %v", err)
	} else {
		t.Log(string(m))
	}
}

func TestMarshalPersistable(t *testing.T) {
	mgr, err := grumble.MakeEntityManager()
	if err != nil {
		t.Errorf("Error creating entity manager: %v", err)
	}
	e, err := mgr.Make(grumble.GetKind(Person{}), grumble.ZeroKey, 0)
	if err != nil {
		t.Errorf("Error creating entity: %v", err)
	}
	jan := e.(*Person)
	jan.FirstName = "Jan"
	jan.LastName = "De Visser"
	jan.Age = 53
	grumble.Initialize(jan, grumble.ZeroKey, 0)
	if m, err := Marshal(jan); err != nil {
		t.Errorf("Error marshaling Jan: %v", err)
	} else {
		t.Log(string(m))
	}
	err = mgr.Put(jan)
	if err != nil {
		t.Errorf("Error writing entity: %v", err)
	}

	e, err = mgr.Make(grumble.GetKind(Person{}), jan.AsKey(), 0)
	if err != nil {
		t.Errorf("Error creating entity: %v", err)
	}
	luc := e.(*Person)
	luc.FirstName = "Luc"
	luc.LastName = "De Visser"
	luc.Age = 13
	if m, err := Marshal(luc); err != nil {
		t.Errorf("Error marshaling Luc: %v", err)
	} else {
		t.Log(string(m))
	}
}
