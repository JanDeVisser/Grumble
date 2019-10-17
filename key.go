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
	Ident     int
	populated bool
	synthetic map[string]interface{}
	mgr       *EntityManager
}

// -- F A C T O R Y  M E T H O D S ------------------------------------------

var ZeroKey = &Key{parent: nil, kind: nil, Ident: 0}

func CreateKey(parent *Key, kind *Kind, id int) (*Key, error) {
	ret := new(Key)
	if parent == nil {
		parent = ZeroKey
	}
	ret.parent = parent
	ret.kind = kind
	ret.Ident = id
	return ret, nil
}

func buildKeyChain(chain []string) (ret *Key, err error) {
	if len(chain) == 0 {
		err = errors.New("key chain is empty")
		return
	}
	keys := len(chain) / 2
	if keys*2 != len(chain) {
		err = errors.New(fmt.Sprintf("key chain '%s' has an odd number of entries", strings.Join(chain, ",")))
		return
	}
	ret = ZeroKey
	for i := keys - 1; i >= 0; i-- {
		k := chain[i*2]
		i := chain[i*2+1]

		if k == "" {
			ret = ZeroKey
			continue
		}
		kind := GetKind(k)
		if kind == nil {
			err = errors.New(fmt.Sprintf("kind '%s' does not exist", k))
			return
		}
		var id int64
		id, err = strconv.ParseInt(i, 0, 0)
		if err != nil {
			return
		}
		ret, err = CreateKey(ret, kind, int(id))
		if err != nil {
			return
		}
	}
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

func (key *Key) Initialize(parent Persistable, id int) *Key {
	if parent != nil {
		key.parent = parent.AsKey()
		key.mgr = parent.Manager()
	}
	key.Ident = id
	return key
}

func (key *Key) SetKind(k *Kind) {
	//if key.kind != nil && key.kind.Kind != k.Kind {
	//	panic(fmt.Sprintf("Attempt to change entity Kind from '%s' to '%s'", key.kind.Kind, k.Kind))
	//}
	key.kind = k
}

func (key *Key) Kind() *Kind {
	if key == nil {
		return nil
	}
	return key.kind
}

func (key *Key) Parent() *Key {
	if key == nil {
		return nil
	}
	return key.parent
}

func (key *Key) AsKey() *Key {
	if key == nil {
		return nil
	}
	return key
}

func (key *Key) IsZero() bool {
	if key == nil {
		return false
	}
	return key.kind == nil
}

func (key *Key) Id() int {
	if key == nil {
		return 0
	}
	return key.Ident
}

func (key *Key) Self() (ret Persistable, err error) {
	return key.mgr.Make(key.Kind(), key.parent, key.Id())
	//ret, err = key.Kind().Make(key.parent, key.Id())
	//if err != nil {
	//	return
	//}
	//return Get(ret, key.Id())
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
		return "(\"\",0)"
	} else {
		return fmt.Sprintf("(%s,%d)", key.Kind().Name(), key.Ident)
	}
}

func (key *Key) Chain() string {
	keys := make([]string, 0)
	for k := key; k != nil && !k.IsZero(); k = k.Parent() {
		keys = append(keys, fmt.Sprintf("%q", k))
	}
	return fmt.Sprintf("{%s}", strings.Join(keys, ","))
}

func (key *Key) SetManager(mgr *EntityManager) {
	key.mgr = mgr
}

func (key *Key) Manager() *EntityManager {
	if key == nil {
		return nil
	}
	return key.mgr
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
