package grumble

type IntField int

type PersistableField interface {
	toSql() interface{}
	fromSql(interface{}) interface{}
}

type Entity struct {
	Parent Key
	Key Key
	Trail *AuditTrail
}

func Put(obj interface{}) bool {
	k := GetKind(obj)
	return k != nil
}

func (i IntField) toSql() interface{} {
	return int(i)
}

func (i IntField) fromSql(interface{}) interface{} {
	return int(i)
}

