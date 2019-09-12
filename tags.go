package grumble

import (
	"fmt"
	"strconv"
	"strings"
)

type Tags struct {
	stringTags map[string]string
	tags       map[string]interface{}
}

func ParseTags(source string) (tags *Tags) {
	tags = &Tags{}
	tags.stringTags = make(map[string]string)
	tags.tags = make(map[string]interface{})
	tagArray := strings.Split(source, ";")
	for _, t := range tagArray {
		t := strings.TrimSpace(t)
		if t != "" {
			nameValue := strings.SplitN(t, "=", 2)
			name := strings.ToLower(strings.TrimSpace(nameValue[0]))
			if len(nameValue) == 2 {
				tags.stringTags[name] = strings.TrimSpace(nameValue[1])
			} else {
				tags.stringTags[name] = "true"
				tags.tags[name] = true
			}
		}
	}
	return
}

func (tags *Tags) Get(name string) string {
	return tags.stringTags[name]
}

func (tags *Tags) Has(name string) (ret bool) {
	_, ret = tags.stringTags[name]
	return
}

func (tags *Tags) GetBool(name string) (ret bool, ok bool) {
	val, ok := tags.tags[name]
	if ok {
		ret, ok = val.(bool)
	} else {
		var s string
		s, ok = tags.stringTags[name]
		if ok {
			var err error
			if ret, err = strconv.ParseBool(s); err != nil {
				ok = false
			}
		}
	}
	return
}

func (tags *Tags) GetInt(name string) (ret int, ok bool) {
	val, ok := tags.tags[name]
	if ok {
		ret, ok = val.(int)
	} else {
		var s string
		s, ok = tags.stringTags[name]
		if ok {
			if ret64, err := strconv.ParseInt(s, 0, 0); err != nil {
				ok = false
			} else {
				ret = int(ret64)
			}
		}
	}
	return
}

func (tags *Tags) Set(name string, val interface{}) {
	tags.stringTags[name] = fmt.Sprintf("%v", val)
	tags.tags[name] = val
}
