package grumble

type Persistable interface {
	Initialize(string, int) *Key
	SetKind(*Kind)
	Kind() *Kind
	Parent() *Key
	AsKey() Key
	Id() int
	Self() (Persistable, error)
	Populated() bool
	SetPopulated()
	SyntheticField(string) (interface{}, bool)
	SetSyntheticField(string, interface{})
}
