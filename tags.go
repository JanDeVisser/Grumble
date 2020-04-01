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

func MakeTags() (tags *Tags) {
	tags = &Tags{}
	tags.stringTags = make(map[string]string)
	tags.tags = make(map[string]interface{})
	return tags
}

func ParseTags(source string) (tags *Tags) {
	tags = MakeTags()
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

func (tags *Tags) Merge(other *Tags) {
	for name, value := range other.stringTags {
		tags.stringTags[name] = value
	}
	for name, value := range other.tags {
		tags.tags[name] = value
	}
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
			} else {
				tags.Set(name, ret)
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
				tags.Set(name, ret)
			}
		}
	}
	return
}

func (tags *Tags) GetStringList(name string) (ret []string) {
	val, ok := tags.tags[name]
	if ok {
		ret, ok = val.([]string)
	} else {
		var s string
		s, ok = tags.stringTags[name]
		if ok {
			ret = strings.Split(s, ",")
			tags.Set(name, ret)
		} else {
			ret = make([]string, 0)
		}
	}
	return
}

func (tags *Tags) Put(name string, value string) {
	delete(tags.tags, name)
	tags.stringTags[name] = value
}

func (tags *Tags) Set(name string, val interface{}) {
	if _, ok := tags.stringTags[name]; !ok {
		tags.stringTags[name] = fmt.Sprintf("%v", val)
	}
	tags.tags[name] = val
}

func (tags *Tags) Append(args ...interface{}) (ret *Tags) {
	ret = tags
	key := ""
	ok := true
	for ix, arg := range args {
		if ix%2 == 0 {
			if key, ok = arg.(string); !ok {
				key = ""
			}
		} else if key != "" {
			ret.Set(key, arg)
		}
	}
	return
}

func (tags *Tags) Tags() (ret map[string]string) {
	return tags.stringTags
}
