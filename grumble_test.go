package grumble

import (
	"reflect"
	"testing"
)

const KIND_NAME = "github.com.jandevisser.grumble.dummyentity"
const DERIVED_NAME = "github.com.jandevisser.grumble.derivedentity"
const ENTITY_ID = 42

type DummyEntity struct {
	Key
	PersistedFld int `grumble:"persist=true"`
	NotPersisted int
	notExported  int
}

type SomeStruct struct {
	x int
}

type DerivedEntity struct {
	DummyEntity
	SomeStruct
	Specialized string `grumble:"persist=true"`
}

type ReferencingEntity struct {
	Key
	Something string       `grumble:"persist=true"`
	Reference *DummyEntity `grumble:"persist"`
}

func (dummy *DummyEntity) dummy(x int) int {
	dummy.PersistedFld = 2 * x
	return dummy.PersistedFld
}

func TestGetKindForType(t *testing.T) {
	k := GetKindForType(reflect.TypeOf(DummyEntity{}))
	if k == nil {
		t.Fatal("Could not get kind for DummyEntity")
	}
	if k.Name() != KIND_NAME {
		t.Errorf("Name '%s' != '%s'", k.Name(), KIND_NAME)
	}
}

func TestGetKindForType_Subclass(t *testing.T) {
	k := GetKindForType(reflect.TypeOf(DerivedEntity{}))
	if k == nil {
		t.Fatal("Could not get kind for DerivedEntity")
	}
	if k.Name() != DERIVED_NAME {
		t.Errorf("Name '%s' != '%s'", k.Name(), DERIVED_NAME)
	}
}

func TestGetKindForType_Reference(t *testing.T) {
	k := GetKindForType(reflect.TypeOf(ReferencingEntity{}))
	if k == nil {
		t.Fatal("Could not get kind for ReferencingEntity")
	}
}

func TestGetKindForKind(t *testing.T) {
	k := GetKindForKind(KIND_NAME)
	if k == nil {
		t.Fatal("Can't get Kind")
	}
	if k.Name() != KIND_NAME {
		t.Errorf("Name '%s' != '%s'", k.Name(), KIND_NAME)
	}
}

func TestCreateKey(t *testing.T) {
	key, err := CreateKey("", GetKindForKind(KIND_NAME), ENTITY_ID)
	if err != nil {
		t.Fatal(err)
	}
	id := key.Id()
	if id != ENTITY_ID {
		t.Errorf("Name '%d' != '%d'", id, ENTITY_ID)
	}
}

func TestEntity_SetKind(t *testing.T) {
	dummy := DummyEntity{}
	SetKind(&dummy)
	if dummy.Kind().Kind != KIND_NAME {
		t.Fatalf("Kind name '%s' != '%s'", dummy.Kind().Kind, KIND_NAME)
	}
	dummyPtr := &DummyEntity{}
	SetKind(dummyPtr)
	if dummyPtr.Kind().Kind != KIND_NAME {
		t.Fatalf("Kind name '%s' != '%s' [Pointer]", dummyPtr.Kind().Kind, KIND_NAME)
	}
}

func TestNewEntityNoParent(t *testing.T) {
	k := GetKindForKind(KIND_NAME)
	if k == nil {
		t.Fatal("Can't get Kind")
	}
	e, err := k.Make("", ENTITY_ID)
	if err != nil {
		t.Fatalf("Can't create entity: %s", err)
	}
	dummy, ok := e.(*DummyEntity)
	if !ok {
		t.Fatal("Could not cast created Entity to DummyEntity")
	}
	if id := dummy.Id(); id != ENTITY_ID {
		t.Fatalf("Dummy Id() '%d' != '%d'", id, ENTITY_ID)
	}
	if dummy.dummy(42) != 84 {
		t.Fatal("dummy.dummy failed")
	}
}

var myID int

func TestPut_insert(t *testing.T) {
	dummy := &DummyEntity{}
	dummy.PersistedFld = 12
	if err := Put(dummy); err != nil {
		t.Fatalf("Could not persist dummy entity: %s", err)
	}
	myID = dummy.Id()
	t.Log(myID)
}

func TestGet_1(t *testing.T) {
	dummy := &DummyEntity{}
	_, err := Get(dummy, myID)
	if err != nil {
		t.Fatalf("Could not Get(%d): %s", myID, err)
	}
	if dummy.Kind().Kind != KIND_NAME {
		t.Errorf("Entity does not have proper Kind after Get: '%s' != '%s'", dummy.Kind().Kind, KIND_NAME)
	}
	if dummy.PersistedFld != 12 {
		t.Fatalf("Entity's fields not restored properly: %d != %d", dummy.PersistedFld, 12)
	}
}

func TestPut_update(t *testing.T) {
	dummy := &DummyEntity{}
	_, err := Get(dummy, myID)
	if err != nil {
		t.Fatalf("Could not Get(%d): %s", myID, err)
	}
	dummy.PersistedFld = 13
	if err := Put(dummy); err != nil {
		t.Fatalf("Could not persist dummy entity: %s", err)
	}
}

func TestGet_2(t *testing.T) {
	dummy := &DummyEntity{}
	_, err := Get(dummy, myID)
	if err != nil {
		t.Fatalf("Could not Get(%d): %s", myID, err)
	}
	if dummy.PersistedFld != 13 {
		t.Fatalf("Entity's fields not restored properly: %d != %d", dummy.PersistedFld, 13)
	}
}

func TestGet_ByKey(t *testing.T) {
	key, err := CreateKey("", GetKindForKind(KIND_NAME), myID)
	if err != nil {
		t.Fatal(err)
	}
	var dummy *DummyEntity
	e, err := Get(key, myID)
	if err != nil {
		t.Fatalf("Could not Get(%d) by Key: %s", myID, err)
	}
	t.Logf("e: %q (%T)", e, e)
	dummy, ok := e.(*DummyEntity)
	if !ok {
		t.Fatalf("Could not cast Persistable returned by Get() by key")
	}
	if dummy.PersistedFld != 13 {
		t.Fatalf("Entity's fields not restored properly: %d != %d", dummy.PersistedFld, 13)
	}
}

func TestMakeQuery(t *testing.T) {
	q := MakeQuery(&DummyEntity{})
	q.AddFilter("PersistedFld", 13)
	q.AddFilter("Bogus", 42)
	t.Log(q.SQL())
}

func TestMakeQuery_WithDerived(t *testing.T) {
	q := MakeQuery(&DummyEntity{})
	q.WithDerived = true
	q.AddFilter("PersistedFld", 13)
	q.AddFilter("Bogus", 42)
	t.Log(q.SQL())
}

func TestMakeQuery_WithReference(t *testing.T) {
	q := MakeQuery(&ReferencingEntity{})
	q.WithDerived = true
	q.AddFilter("Something", "jan")
	t.Log(q.SQL())
}

func TestMakeQuery_WithJoin(t *testing.T) {
	q := MakeQuery(&ReferencingEntity{})
	q.WithDerived = true
	q.AddFilter("Something", "jan")
	join := Join{QueryTable: QueryTable{Kind: GetKindForKind(KIND_NAME)}, FieldName: "Reference"}
	q.AddJoin(join)
	t.Log(q.SQL())
}
