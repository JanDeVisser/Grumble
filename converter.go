package grumble

import (
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"strconv"
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

type Setter interface {
	SetValue(Persistable, Column, interface{}) error
}

type BasicConverter struct {
	sqlType string
	GoType  reflect.Type
}

var ConvertersForKind = map[reflect.Kind]Converter{
	reflect.Bool:    &BasicConverter{"boolean", reflect.TypeOf(true)},
	reflect.Int:     &BasicConverter{"integer", reflect.TypeOf(1)},
	reflect.Int8:    &BasicConverter{"integer", reflect.TypeOf(int8(1))},
	reflect.Int16:   &BasicConverter{"integer", reflect.TypeOf(int16(1))},
	reflect.Int32:   &BasicConverter{"integer", reflect.TypeOf(int32(1))},
	reflect.Int64:   &BasicConverter{"integer", reflect.TypeOf(int64(1))},
	reflect.Uint:    &BasicConverter{"integer", reflect.TypeOf(uint(1))},
	reflect.Uint8:   &BasicConverter{"integer", reflect.TypeOf(uint8(1))},
	reflect.Uint16:  &BasicConverter{"integer", reflect.TypeOf(uint16(1))},
	reflect.Uint32:  &BasicConverter{"integer", reflect.TypeOf(uint32(1))},
	reflect.Uint64:  &BasicConverter{"integer", reflect.TypeOf(uint64(1))},
	reflect.Float32: &BasicConverter{"double precision", reflect.TypeOf(float32(1.0))},
	reflect.Float64: &BasicConverter{"double precision", reflect.TypeOf(float64(1.0))},
	reflect.String:  &BasicConverter{"text", reflect.TypeOf("")},
}

var ConvertersForType = map[reflect.Type]Converter{
	reflect.TypeOf(time.Time{}): &BasicConverter{"timestamp without time zone", reflect.TypeOf(time.Time{})},
}

func GetConverterForType(t reflect.Type) (converter Converter) {
	switch {
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8:
		converter = &BasicConverter{sqlType: "bytea"}
	case ConvertersForType[t] != nil:
		converter = ConvertersForType[t]
	case ConvertersForKind[t.Kind()] != nil:
		converter = ConvertersForKind[t.Kind()]
	}
	return
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

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

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

func (converter *BasicConverter) SetValue(e Persistable, column Column, value interface{}) (err error) {
	v := reflect.ValueOf(e)
	if v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	fld := v.FieldByIndex(column.Index)
	if fld.IsValid() {
		defer func() {
			if e := recover(); e != nil {
				err = errors.New(fmt.Sprintf("%v", e))
			}
			return
		}()
		switch vv := value.(type) {
		case string:
			switch fld.Kind() {
			case reflect.String:
				fld.Set(reflect.ValueOf(value))
			case reflect.Int:
				if i, err := strconv.ParseInt(vv, 10, 64); err != nil {
					panic(fmt.Sprintf("can't convert %q to int", vv))
				} else {
					fld.Set(reflect.ValueOf(int(i)))
				}
			case reflect.Int32:
				if i, err := strconv.ParseInt(vv, 10, 32); err != nil {
					panic(fmt.Sprintf("can't convert %q to int32", vv))
				} else {
					fld.Set(reflect.ValueOf(int32(i)))
				}
			case reflect.Int64:
				if i, err := strconv.ParseInt(vv, 10, 32); err != nil {
					panic(fmt.Sprintf("can't convert %q to int64", vv))
				} else {
					fld.Set(reflect.ValueOf(i))
				}
			case reflect.Float32:
				if f, err := strconv.ParseFloat(vv, 32); err != nil {
					panic(fmt.Sprintf("can't convert %q to float32", vv))
				} else {
					fld.Set(reflect.ValueOf(float32(f)))
				}
			case reflect.Float64:
				if f, err := strconv.ParseFloat(vv, 64); err != nil {
					panic(fmt.Sprintf("can't convert %q to float64", vv))
				} else {
					fld.Set(reflect.ValueOf(f))
				}
			case reflect.Bool:
				if b, err := strconv.ParseBool(vv); err != nil {
					panic(fmt.Sprintf("can't convert %q to bool", vv))
				} else {
					fld.Set(reflect.ValueOf(b))
				}
			case reflect.Struct:
				if converter.GoType == reflect.TypeOf(time.Time{}) {
					if t, err := time.Parse("2006-01-02", vv); err != nil {
						panic(fmt.Sprintf("can't convert %q to Time", vv))
					} else {
						fld.Set(reflect.ValueOf(t))
					}
				} else {
					log.Println(column.ColumnName, vv)
				}
			default:
				panic(fmt.Sprintf("cannot convert strings to objects of kind %q", fld.Kind()))
			}
		case float64:
			switch fld.Kind() {
			case reflect.String:
				fld.Set(reflect.ValueOf(strconv.FormatFloat(vv, 'g', -1, 64)))
			case reflect.Int:
				fld.Set(reflect.ValueOf(int(math.Round(vv))))
			case reflect.Int32:
				fld.Set(reflect.ValueOf(int32(math.Round(vv))))
			case reflect.Int64:
				fld.Set(reflect.ValueOf(int64(math.Round(vv))))
			case reflect.Float32:
				fld.Set(reflect.ValueOf(float32(vv)))
			case reflect.Float64:
				fld.Set(reflect.ValueOf(vv))
			case reflect.Bool:
				fld.Set(reflect.ValueOf(vv != 0.0))
			default:
				panic(fmt.Sprintf("cannot convert floats to objects of kind %q", fld.Kind()))
			}
		case float32:
			vv64 := float64(vv)
			switch fld.Kind() {
			case reflect.String:
				fld.Set(reflect.ValueOf(strconv.FormatFloat(vv64, 'g', -1, 32)))
			case reflect.Int:
				fld.Set(reflect.ValueOf(int(math.Round(vv64))))
			case reflect.Int32:
				fld.Set(reflect.ValueOf(int32(math.Round(vv64))))
			case reflect.Int64:
				fld.Set(reflect.ValueOf(int64(math.Round(vv64))))
			case reflect.Float32:
				fld.Set(reflect.ValueOf(vv))
			case reflect.Float64:
				fld.Set(reflect.ValueOf(vv64))
			case reflect.Bool:
				fld.Set(reflect.ValueOf(vv != 0.0))
			default:
				panic(fmt.Sprintf("cannot convert floats to objects of kind %q", fld.Kind()))
			}
		case int:
			switch fld.Kind() {
			case reflect.String:
				fld.Set(reflect.ValueOf(strconv.FormatInt(int64(vv), 10)))
			case reflect.Int:
				fld.Set(reflect.ValueOf(vv))
			case reflect.Int32:
				fld.Set(reflect.ValueOf(int32(vv)))
			case reflect.Int64:
				fld.Set(reflect.ValueOf(int64(vv)))
			case reflect.Float32:
				fld.Set(reflect.ValueOf(float32(vv)))
			case reflect.Float64:
				fld.Set(reflect.ValueOf(float64(vv)))
			case reflect.Bool:
				fld.Set(reflect.ValueOf(vv != 0))
			default:
				panic(fmt.Sprintf("cannot convert integers to objects of kind %q", fld.Kind()))
			}
		case int32:
			switch fld.Kind() {
			case reflect.String:
				fld.Set(reflect.ValueOf(strconv.FormatInt(int64(vv), 10)))
			case reflect.Int:
				fld.Set(reflect.ValueOf(int(vv)))
			case reflect.Int32:
				fld.Set(reflect.ValueOf(vv))
			case reflect.Int64:
				fld.Set(reflect.ValueOf(int64(vv)))
			case reflect.Float32:
				fld.Set(reflect.ValueOf(float32(vv)))
			case reflect.Float64:
				fld.Set(reflect.ValueOf(float64(vv)))
			case reflect.Bool:
				fld.Set(reflect.ValueOf(vv != 0))
			default:
				panic(fmt.Sprintf("cannot convert integers to objects of kind %q", fld.Kind()))
			}
		case int64:
			switch fld.Kind() {
			case reflect.String:
				fld.Set(reflect.ValueOf(strconv.FormatInt(vv, 10)))
			case reflect.Int:
				fld.Set(reflect.ValueOf(int(vv)))
			case reflect.Int32:
				fld.Set(reflect.ValueOf(int32(vv)))
			case reflect.Int64:
				fld.Set(reflect.ValueOf(vv))
			case reflect.Float32:
				fld.Set(reflect.ValueOf(float32(vv)))
			case reflect.Float64:
				fld.Set(reflect.ValueOf(float64(vv)))
			case reflect.Bool:
				fld.Set(reflect.ValueOf(vv != 0))
			default:
				panic(fmt.Sprintf("cannot convert integers to objects of kind %q", fld.Kind()))
			}
		default:
			fld.Set(reflect.ValueOf(vv))
		}
	}
	return
}

// -- R E F E R E N C E S ---------------------------------------------------

type ReferenceScanner struct {
	KeyScanner
	FieldName   string
	FieldValues map[string]interface{}
}

func (scanner *ReferenceScanner) Scan(src interface{}) (err error) {
	err = scanner.KeyScanner.Scan(src)
	if err != nil {
		return
	}
	var e Persistable
	if scanner.Key.Kind() != nil {
		e, err = scanner.Key.Kind().Make(nil, scanner.Key.Id())
		if err != nil {
			return err
		}
		//e, err = Get(e, scanner.Key.Id())
		//if err != nil {
		//	return err
		//}
	}
	scanner.FieldValues[scanner.FieldName] = e
	return
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

type ReferenceConverter struct {
	References *Kind
}

func (ref *ReferenceConverter) SQLType(column Column) string {
	pg := GetPostgreSQLAdapter()
	return fmt.Sprintf("\"%s\".\"Reference\"", pg.Schema)
}

func (ref *ReferenceConverter) SQLTextOut(column Column) string {
	return "__count__"
}

func (ref *ReferenceConverter) Value(e Persistable, column Column) (values []interface{}, err error) {
	v := reflect.ValueOf(e).Elem()
	fieldValue := v.FieldByIndex(column.Index)
	if !fieldValue.IsNil() {
		value := fieldValue.Interface()
		if reference, ok := value.(Persistable); ok {
			kind := reference.Kind()
			if !kind.DerivesFrom(ref.References) {
				err = errors.New(fmt.Sprintf("Kind '%s' does not derive from '%s'", kind.Kind, ref.References.Kind))
				return
			}
			values = []interface{}{reference.AsKey().String()}
		} else {
			err = errors.New(fmt.Sprintf("column '%s' value is not a Persistable", column.FieldName))
			return
		}
	} else {
		values = []interface{}{ZeroKey.String()}
	}
	return
}

func (ref *ReferenceConverter) SQLTextIn(column Column, alias string, with bool) string {
	if alias != "" {
		alias = alias + "."
	}
	return fmt.Sprintf("%s%q", alias, column.ColumnName)
}

func (ref *ReferenceConverter) Scanners(column Column, scanners []interface{}, values map[string]interface{}) ([]interface{}, error) {
	scanner := new(ReferenceScanner)
	scanner.Expects = ref.References
	scanner.FieldName = column.FieldName
	scanner.FieldValues = values
	return append(scanners, scanner), nil
}

func (ref *ReferenceConverter) SetValue(e Persistable, column Column, value interface{}) error {
	ev := reflect.ValueOf(e).Elem()
	fld := ev.FieldByIndex(column.Index)

	var err error
	var reference Persistable
	switch r := value.(type) {
	case Key:
		reference, err = r.Self()
		if err != nil {
			return err
		}
	case Persistable:
		reference = r
	case string:
		k, err := ParseKey(r)
		if err == nil {
			return ref.SetValue(e, column, k)
		} else {
			id, err := strconv.ParseInt(r, 10, 64)
			if err != nil {
				return err
			}
			return ref.SetValue(e, column, int(id))
		}
	case int64:
		return ref.SetValue(e, column, int(r))
	case int:
		reference, err = e.Manager().Get(ref.References, r)
		if err != nil {
			return err
		}
	}

	v := reflect.ValueOf(reference).Elem()
	var k = reference.Kind()
	for ; k != nil && k.Kind != ref.References.Kind; k = k.BaseKind {
		v = v.FieldByIndex([]int{k.baseIndex})
	}
	if k == nil {
		return errors.New(fmt.Sprintf("can't assign entity of kind '%s' to column %s.%s",
			reference.Kind().Kind, column.Kind.Kind, column.FieldName))
	}
	fld.Set(reflect.ValueOf(reference))
	return nil
}
