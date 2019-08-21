package grumble

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type AuditTrail struct {
	Created time.Time
	CreatedBy string
	Updated time.Time
	UpdatedBy string
}

type KeyError struct {
	Key string
	_err string
}

func CreateKeyError(key string, errorText string) KeyError {
	ret := KeyError{key, errorText}
	return ret
}

func (e KeyError) Error() string {
	return e._err
}

type Key struct {
	Parent *Key
	Kind *Kind
	Name string
}

func CreateKey(parent *Key, kind *Kind, name string) *Key {
	ret := new(Key)
	ret.Parent = parent
	ret.Kind = kind
	ret.Name = name
	return ret
}

func ParseKey(key string) (*Key, error) {
	if key == "" {
		return nil, CreateKeyError("", "Cannot parse empty key")
	}
	var parent *Key
	var local string
	var err error
	if lastSlash := strings.LastIndex(key, "/"); lastSlash > 0 {
		if parent, err = ParseKey(key[:lastSlash]); err != nil {
			return nil, CreateKeyError(key, err.Error())
		}
		local = key[lastSlash+1:]
	} else {
		parent = nil
		local = key
	}
	kindName := strings.Split(local, ":")
	kind := GetKindForKind(kindName[0])
	var name string
	if name, err = url.QueryUnescape(kindName[1]); err != nil {
		return nil, CreateKeyError(key, err.Error())
	}
	return CreateKey(parent, kind, name), nil
}

func (k Key) String() string {
	local := fmt.Sprintf("%s:%s", k.Kind.Name(), url.QueryEscape(k.Name))
	switch k.Parent {
	case nil:
		return local
	default:
		return fmt.Sprintf("%s/%s", k.Parent.String(), local)
	}
}
