package grumble

import (
	"reflect"
	"testing"
)

const KIND_NAME = "grumble.dummyentity"
const ENTITY_KEY = "SampleKey"

type DummyEntity struct {
	PersistedFld int `persistable:"true"`
	NotPersisted int
	notExported int
}

func TestGetKindForType(t *testing.T) {
	k := GetKindForType(reflect.TypeOf(DummyEntity{}))
	if k.Name() != "grumble.dummyentity" {
		t.Errorf("Name '%s' != '%s'", k.Name(), KIND_NAME)
	}
}

func TestGetKindForKind(t *testing.T) {
	var k *Kind
	k = GetKindForKind(KIND_NAME)
	if k.Name() != "grumble.dummyentity" {
		t.Errorf("Name '%s' != '%s'", k.Name(), KIND_NAME)
	}
}

func TestCreateKey(t *testing.T) {
	var key *Key
	key = CreateKey(nil, GetKindForKind(KIND_NAME), ENTITY_KEY)
	name := key.Name
	if name != ENTITY_KEY {
		t.Errorf("Name '%s' != '%s'", name, ENTITY_KEY)
	}
}
