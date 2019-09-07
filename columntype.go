package grumble

import (
	"errors"
	"fmt"
	"reflect"
)

type ColumnType interface {
	SQLType(Column) string
	SQLTextOut(Column) string
	Value(Persistable, Column) ([]interface{}, error)
	SQLTextIn(Column, string, bool) string
	Scanners(Column, []interface{}, map[string]interface{}) ([]interface{}, error)
}

var ColumnTypes = map[reflect.Kind]ColumnType{
	reflect.Bool:    &BasicColumnType{"boolean"},
	reflect.Int:     &BasicColumnType{"integer"},
	reflect.Int8:    &BasicColumnType{"integer"},
	reflect.Int16:   &BasicColumnType{"integer"},
	reflect.Int32:   &BasicColumnType{"integer"},
	reflect.Int64:   &BasicColumnType{"integer"},
	reflect.Uint:    &BasicColumnType{"integer"},
	reflect.Uint8:   &BasicColumnType{"integer"},
	reflect.Uint16:  &BasicColumnType{"integer"},
	reflect.Uint32:  &BasicColumnType{"integer"},
	reflect.Uint64:  &BasicColumnType{"integer"},
	reflect.Float32: &BasicColumnType{"float"},
	reflect.Float64: &BasicColumnType{"float"},
	reflect.String:  &BasicColumnType{"text"},
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

type BasicColumnType struct {
	sqlType string
}

func (columnType *BasicColumnType) SQLType(column Column) string {
	return columnType.sqlType
}

func (columnType *BasicColumnType) SQLTextOut(column Column) string {
	return "__count__"
}

func (columnType *BasicColumnType) Value(e Persistable, column Column) ([]interface{}, error) {
	v := reflect.ValueOf(e).Elem()
	return []interface{}{v.FieldByIndex(column.Index).Interface()}, nil
}

func (columnType *BasicColumnType) SQLTextIn(column Column, alias string, with bool) string {
	if alias != "" {
		alias = alias + "."
	}
	return fmt.Sprintf("%s\"%s\"", alias, column.ColumnName)
}

func (columnType *BasicColumnType) Scanners(column Column, scanners []interface{}, values map[string]interface{}) ([]interface{}, error) {
	return append(scanners, &BasicScanner{FieldName: column.FieldName, FieldValues: values}), nil
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
		if k, ok := src.(string); ok {
			scanner.kind = GetKindForKind(k)
			if scanner.kind == nil {
				err = errors.New(fmt.Sprintf("unknown kind '%s'", k))
				return
			}
			if !scanner.kind.DerivesFrom(scanner.BaseKind) {
				err = errors.New(fmt.Sprintf("kind '%s' does not derive from '%s'",
					k, scanner.BaseKind.Kind))
				return
			}
		}
	case 1:
		if id, ok := src.(int); ok {
			e, err := scanner.kind.Make("", id)
			if err != nil {
				return err
			}
			scanner.FieldValues[scanner.FieldName] = e
		}
		scanner.count = -1
	}
	scanner.count++
	return nil
}

type ReferenceType struct {
	References *Kind
}

func (ref *ReferenceType) SQLType(column Column) string {
	pg := GetPostgreSQLAdapter()
	return fmt.Sprintf("\"%s\".\"Reference\"", pg.Schema)
}

func (ref *ReferenceType) SQLTextOut(column Column) string {
	return "( __count__, __count__)"
}

func (ref *ReferenceType) Value(e Persistable, column Column) (values []interface{}, err error) {
	v := reflect.ValueOf(e).Elem()
	fieldValue := v.FieldByIndex(column.Index)
	value := fieldValue.Interface()
	if reference, ok := value.(Persistable); ok {
		kind := reference.Kind()
		if !kind.DerivesFrom(column.Kind) {
			err = errors.New(fmt.Sprintf("Kind '%s' does not derive from '%s'", kind.Kind, column.Kind.Kind))
			return
		}
		values = []interface{}{kind.Kind, reference.Id()}
		return
	} else {
		err = errors.New(fmt.Sprintf("column '%s' value is not a Persistable", column.FieldName))
		return
	}
}

func (ref *ReferenceType) SQLTextIn(column Column, alias string, with bool) string {
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

func (ref *ReferenceType) Scanners(column Column, scanners []interface{}, values map[string]interface{}) ([]interface{}, error) {
	scanner := new(ReferenceScanner)
	scanner.BaseKind = ref.References
	scanner.FieldName = column.FieldName
	scanner.FieldValues = values
	return append(scanners, scanner, scanner), nil
}
