package grumble

type Sql struct {
	SqlText string
	Params []interface{}
}

func Load(key Key) (Sql, error) {
	ret := Sql{}
	ret.SqlText = "SELECT e._parent, e._acl, "
	if key.Parent != nil {
		ret.Params = make([]interface{}, 2)
		ret.Params[1] = key.Parent.String()
	} else {
		ret.Params = make([]interface{}, 1)
	}
	ret.Params[0] = key.Name
	//table := key.Kind.TableName
	return ret, nil
}
