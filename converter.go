package grumble

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

type Converter interface {
	SQLType(Column) string
	SQLTextOut(Column) string
	Value(Persistable, Column) ([]interface{}, error)
	SQLTextIn(Column, string, bool) string
	Scanners(Column, []interface{}, map[string]interface{}) ([]interface{}, error)
}

type Adapter interface {
	Converter(field reflect.StructField, tags *Tags) Converter
}

var ConvertersForKind = map[reflect.Kind]Converter{
	reflect.Bool:    &BasicConverter{"boolean"},
	reflect.Int:     &BasicConverter{"integer"},
	reflect.Int8:    &BasicConverter{"integer"},
	reflect.Int16:   &BasicConverter{"integer"},
	reflect.Int32:   &BasicConverter{"integer"},
	reflect.Int64:   &BasicConverter{"integer"},
	reflect.Uint:    &BasicConverter{"integer"},
	reflect.Uint8:   &BasicConverter{"integer"},
	reflect.Uint16:  &BasicConverter{"integer"},
	reflect.Uint32:  &BasicConverter{"integer"},
	reflect.Uint64:  &BasicConverter{"integer"},
	reflect.Float32: &BasicConverter{"float"},
	reflect.Float64: &BasicConverter{"float"},
	reflect.String:  &BasicConverter{"text"},
}

var ConvertersForType = map[reflect.Type]Converter{
	reflect.TypeOf(time.Time{}): &BasicConverter{"timestamp"},
}

// -- B A S I C  T Y P E S --------------------------------------------------

type BasicScanner struct {
	FieldName   string
	FieldValues map[string]interface{}
}

func (scanner *BasicScanner) Scan(value interface{}) (err error) {
	if v, ok := value.(int64); ok { // HACK should deal with 64 bitidy better
		value = int(v)
	}
	scanner.FieldValues[scanner.FieldName] = value
	return
}

type BasicConverter struct {
	sqlType string
}

func (converter *BasicConverter) SQLType(column Column) string {
	return converter.sqlType
}

func (converter *BasicConverter) SQLTextOut(column Column) string {
	return "__count__"
}

func (converter *BasicConverter) Value(e Persistable, column Column) ([]interface{}, error) {
	v := reflect.ValueOf(e).Elem()
	return []interface{}{v.FieldByIndex(column.Index).Interface()}, nil
}

func (converter *BasicConverter) SQLTextIn(column Column, alias string, with bool) string {
	if alias != "" {
		alias = alias + "."
	}
	return fmt.Sprintf("%s\"%s\"", alias, column.ColumnName)
}

func (converter *BasicConverter) Scanners(column Column, scanners []interface{}, values map[string]interface{}) ([]interface{}, error) {
	return append(scanners, &BasicScanner{FieldName: column.FieldName, FieldValues: values}), nil
}

// -- S E T T E R -----------------------------------------------------------

type Setter interface {
	SetValue(Persistable, Column, interface{}) error
}

// -- R E F E R E N C E S ---------------------------------------------------

type ReferenceScanner struct {
	FieldName   string
	FieldValues map[string]interface{}
	BaseKind    *Kind
	kind        *Kind
	count       int
}

func (scanner *ReferenceScanner) Scan(src interface{}) (err error) {
	switch scanner.count {
	case 0:
		scanner.kind = nil
		switch k := src.(type) {
		case string:
			kind := GetKindForKind(k)
			switch {
			case kind == nil:
				err = errors.New(fmt.Sprintf("unknown kind '%s'", k))
			case !kind.DerivesFrom(scanner.BaseKind):
				err = errors.New(fmt.Sprintf("kind '%s' does not derive from '%s'",
					k, scanner.BaseKind.Kind))
			default:
				scanner.kind = kind
			}
		case nil:
			scanner.kind = nil
		default:
			err = errors.New(fmt.Sprintf("ReferenceScanner expected string kind, not '%q', (%T)", src, src))
		}
	case 1:
		scanner.count = -1
		switch id := src.(type) {
		case int64:
			e, err := scanner.kind.Make(nil, int(id))
			if err != nil {
				return err
			}
			scanner.FieldValues[scanner.FieldName] = e
		case nil:
			scanner.FieldValues[scanner.FieldName] = nil
		default:
			err = errors.New(fmt.Sprintf("ReferenceScanner expected string kind, not '%q', (%T)", src, src))
		}
	}
	scanner.count++
	return
}

type ReferenceConverter struct {
	References *Kind
}

func (ref *ReferenceConverter) SQLType(column Column) string {
	pg := GetPostgreSQLAdapter()
	return fmt.Sprintf("\"%s\".\"Reference\"", pg.Schema)
}

func (ref *ReferenceConverter) SQLTextOut(column Column) string {
	return "( __count__, __count__)"
}

func (ref *ReferenceConverter) Value(e Persistable, column Column) (values []interface{}, err error) {
	v := reflect.ValueOf(e).Elem()
	fieldValue := v.FieldByIndex(column.Index)
	value := fieldValue.Interface()
	if reference, ok := value.(Persistable); ok {
		kind := reference.Kind()
		if !kind.DerivesFrom(ref.References) {
			err = errors.New(fmt.Sprintf("Kind '%s' does not derive from '%s'", kind.Kind, ref.References.Kind))
			return
		}
		values = []interface{}{kind.Kind, reference.Id()}
		return
	} else {
		err = errors.New(fmt.Sprintf("column '%s' value is not a Persistable", column.FieldName))
		return
	}
}

func (ref *ReferenceConverter) SQLTextIn(column Column, alias string, with bool) string {
	if alias != "" {
		alias = alias + "."
	}
	if with {
		return fmt.Sprintf("(%s\"%s\").kind \"%sKind\", (%s\"%s\").id \"%sId\"",
			alias, column.ColumnName, column.ColumnName, alias, column.ColumnName, column.ColumnName)
	} else {
		return fmt.Sprintf("%s\"%sKind\", %s\"%sId\"", alias, column.ColumnName, alias, column.ColumnName)
	}
}

func (ref *ReferenceConverter) Scanners(column Column, scanners []interface{}, values map[string]interface{}) ([]interface{}, error) {
	scanner := new(ReferenceScanner)
	scanner.BaseKind = ref.References
	scanner.FieldName = column.FieldName
	scanner.FieldValues = values
	return append(scanners, scanner, scanner), nil
}

func (ref *ReferenceConverter) SetValue(e Persistable, column Column, value interface{}) error {
	ev := reflect.ValueOf(e).Elem()
	fld := ev.FieldByIndex(column.Index)

	reference, ok := value.(Persistable)
	if !ok {
		return errors.New(fmt.Sprintf("can't assign values of type '%T' to column %s.%s",
			value, column.Kind.Kind, column.FieldName))
	}
	v := reflect.ValueOf(value).Elem()
	var k = reference.Kind()
	for ; k != nil && k.Kind != ref.References.Kind; k = k.base {
		v = v.FieldByIndex([]int{k.baseIndex})
	}
	if k == nil {
		return errors.New(fmt.Sprintf("can't assign entity of kind '%s' to column %s.%s",
			reference.Kind().Kind, column.Kind.Kind, column.FieldName))
	}
	fld.Set(v.Addr())
	return nil
}
