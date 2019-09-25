package grumble

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Key struct {
	kind      *Kind
	parent    *Key
	id        int
	populated bool
	synthetic map[string]interface{}
}

// -- F A C T O R Y  M E T H O D S ------------------------------------------

var ZeroKey = &Key{parent: nil, kind: nil, id: 0}

func CreateKey(parent *Key, kind *Kind, id int) (*Key, error) {
	ret := new(Key)
	if parent == nil {
		parent = ZeroKey
	}
	ret.parent = parent
	ret.kind = kind
	ret.id = id
	return ret, nil
}

func buildKeyChain(chain []string) (ret *Key, err error) {
	if len(chain) < 2 {
		return
	}
	k := chain[len(chain)-2]
	i := chain[len(chain)-1]
	p, err := buildKeyChain(chain[:len(chain)-2])
	if err != nil {
		return
	}

	if k == "#zero#" {
		return ZeroKey, nil
	}
	kind := GetKind(k)
	if kind == nil {
		err = errors.New(fmt.Sprintf("kind '%s' does not exist", k))
		return
	}
	id, err := strconv.ParseInt(i, 0, 0)
	if err != nil {
		return
	}
	ret, err = CreateKey(p, kind, int(id))
	return
}

func ParseKey(key string) (*Key, error) {
	r := strings.NewReplacer("{", "", "}", "", "(", "", ")", "", " ", "", "\"", "")
	key = r.Replace(key)
	if key == "" {
		return ZeroKey, nil
	}
	return buildKeyChain(strings.Split(key, ","))
}

// -- P E R S I S T A B L E  I M P L E M E N T A T I O N --------------------

func (key *Key) Initialize(parent *Key, id int) *Key {
	if parent == nil {
		parent = ZeroKey
	}
	key.parent = parent
	key.id = id
	return key
}

func (key *Key) SetKind(k *Kind) {
	//if key.kind != nil && key.kind.Kind != k.Kind {
	//	panic(fmt.Sprintf("Attempt to change entity Kind from '%s' to '%s'", key.kind.Kind, k.Kind))
	//}
	key.kind = k
}

func (key *Key) Kind() *Kind {
	return key.kind
}

func (key *Key) Parent() *Key {
	return key.parent
}

func (key *Key) AsKey() *Key {
	return key
}

func (key *Key) IsZero() bool {
	return key.kind == nil
}

func (key *Key) Id() int {
	return key.id
}

func (key *Key) Self() (ret Persistable, err error) {
	ret, err = key.Kind().Make(key.parent, key.Id())
	if err != nil {
		return
	}
	return Get(ret, key.Id())
}

func (key *Key) SyntheticField(name string) (ret interface{}, ok bool) {
	ret, ok = key.synthetic[name]
	return
}

func (key *Key) SetSyntheticField(name string, value interface{}) {
	if key.synthetic == nil {
		key.synthetic = make(map[string]interface{})
	}
	key.synthetic[name] = value
}

func (key *Key) Populated() bool {
	return key.populated
}

func (key *Key) SetPopulated() {
	key.populated = true
}

// -- E N D  I M P L E M E N T A T I O N ------------------------------------

func (key *Key) String() string {
	if key.IsZero() {
		return "(\"#zero#\",0)"
	} else {
		return fmt.Sprintf("(%s,%d)", key.Kind().Name(), key.id)
	}
}

func (key *Key) Chain() string {
	keys := make([]string, 0)
	for k := key; k != nil && !k.IsZero(); k = k.Parent() {
		keys = append(keys, fmt.Sprintf("%q", k))
	}
	return fmt.Sprintf("{%s}", strings.Join(keys, ","))
}

func (key *Key) Field(fieldName string) interface{} {
	v := reflect.ValueOf(key).Elem()
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	fld := v.FieldByNameFunc(func(s string) bool {
		return strings.ToLower(s) == strings.ToLower(fieldName)
	})
	if fld.IsValid() && fld.CanInterface() {
		return fld.Interface()
	} else {
		return nil
	}
}

func (key *Key) SetField(fieldName string, value interface{}) {
	v := reflect.ValueOf(key)
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	fld := v.FieldByNameFunc(func(s string) bool {
		return strings.ToLower(s) == strings.ToLower(fieldName)
	})
	if fld.IsValid() {
		fld.Set(reflect.ValueOf(value))
	}
}

func (key *Key) Label() string {
	k := key.Kind()
	if k.LabelCol == "" {
		return key.String()
	} else {
		return fmt.Sprintf("%v", key.Field(k.LabelCol))
	}
}
