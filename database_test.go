package grumble

import (
	"errors"
	"fmt"
	"testing"
)

func TestGetPostgreSQLAdapter(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	pg.Schema = "information_schema"
	pg2 := GetPostgreSQLAdapter()
	if pg.Schema == pg2.Schema {
		t.Errorf("Modifying adapter modifies default")
	}
}

func TestPostgreSQLAdapter_GetConnection(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	conn := pg.GetConnection()
	err := conn.Ping()
	if err != nil {
		t.Fatalf("Could not ping non-admin connection")
	}
	conn = pg.GetAdminConnection()
	err = conn.Ping()
	if err != nil {
		t.Errorf("Could not ping admin connection")
	}
}

func TestPostgreSQLAdapter_ResetSchema(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	if err := pg.ResetSchema(); err != nil {
		t.Error(err)
	}
}

func TestSQLTable_Exists(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	table := pg.makeTable("schemata")
	table.Schema = "information_schema"
	var exists bool
	var err error
	if exists, err = table.Exists(); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Errorf("Table schemata should exist")
	}
}

var TableName = "TestTable"
var TableColumns = []SQLColumn{
	{Name: "primarykey", SQLType: "text", Nullable: false, Default: "", PrimaryKey: true, Unique: true},
	{Name: "indexedcolumn", SQLType: "integer", Nullable: true, Default: "42", Indexed: true},
	{Name: "uniquecolumn", SQLType: "text", Nullable: false, Default: "''", Unique: true},
	{Name: "compoundindex_1", SQLType: "text", Nullable: false, Default: "''", Unique: false},
	{Name: "compoundindex_2", SQLType: "integer", Nullable: false, Default: "12", Unique: false},
}
var TableIndexes = []SQLIndex{
	{
		Columns: []string{"compoundindex_1", "compoundindex_2"},
		Unique:  true,
	},
}

var BogusTable = "BogusTableDoesNotExist"

var AddedColumn = SQLColumn{
	Name:       "addedcolumn",
	SQLType:    "boolean",
	Default:    "true",
	Nullable:   false,
	PrimaryKey: false,
	Unique:     false,
	Indexed:    false,
}

func createTableByDef(t *testing.T, pg *PostgreSQLAdapter, name string, columns []SQLColumn, indexes []SQLIndex) SQLTable {
	table := pg.makeTable(name)
	for _, col := range columns {
		if err := table.AddColumn(col); err != nil {
			t.Fatal(err)
		}
	}
	for _, index := range indexes {
		if err := table.AddIndex(index); err != nil {
			t.Fatal(err)
		}
	}
	return table
}

func createTestTable(t *testing.T, pg *PostgreSQLAdapter) SQLTable {
	return createTableByDef(t, pg, TableName, TableColumns, TableIndexes)
}

func equals(table1 SQLTable, table2 SQLTable) (bool, error) {
	if table1.QualifiedName() != table2.QualifiedName() {
		return false, errors.New(fmt.Sprintf("%s != %s", table1.QualifiedName(), table2.QualifiedName()))
	}
	if len(table1.Columns) != len(table2.Columns) {
		return false, errors.New(fmt.Sprintf("len(Columns): %d != %d", len(table1.Columns), len(table2.Columns)))
	}
	if len(table1.Indexes) != len(table2.Indexes) {
		return false, errors.New(fmt.Sprintf("len(Indexes): %d != %d", len(table1.Indexes), len(table2.Indexes)))
	}
	for _, c1 := range table1.Columns {
		c2 := table2.GetColumnByName(c1.Name)
		if c2 == nil {
			return false, errors.New(fmt.Sprintf("Column '%s' not found in table2", c1.Name))
		}
		if c1 != *c2 {
			return false, errors.New(fmt.Sprintf("Column '%s': %v != %v", c1.Name, c1, *c2))
		}
	}
	for _, i1 := range table1.Indexes {
		i2 := table2.GetIndexByName(i1.Name)
		if i2 == nil {
			return false, errors.New(fmt.Sprintf("Index '%s' not found in table2", i1.Name))
		}
		if i1.Unique != i2.Unique {
			return false, errors.New(fmt.Sprintf("Index '%s'.Unique: %v != %v",
				i1.Name, i1.Unique, i2.Unique))
		}
		if len(i1.Columns) != len(i2.Columns) {
			return false, errors.New(fmt.Sprintf("'%s'.len(IXColumns): %d != %d",
				i1.Name, len(i1.Columns), len(i2.Columns)))
		}
		for ix, col := range i1.Columns {
			if col != i2.Columns[ix] {
				return false, errors.New(fmt.Sprintf("Index '%s'[%d]: %s != %s",
					i1.Name, ix, col, i2.Columns[ix]))
			}
		}
	}
	return true, nil
}

// Create table
func TestSQLTable_Reconcile_1(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	table := createTestTable(t, pg)
	var exists bool
	var err error
	if exists, err = table.Exists(); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Errorf("Table '%s' already exists", TableName)
	}
	err = table.Reconcile()
	if err != nil {
		t.Error(err)
	}
	if exists, err = table.Exists(); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Errorf("Table '%s' was not created", TableName)
	}
}

// Verify that created table is identical to defined one
func TestSQLTable_Sync_1(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	current := pg.makeTable(TableName)
	var exists bool
	var err error
	if exists, err = current.Exists(); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Errorf("Table '%s' does not exists", TableName)
	}
	err = current.Sync()
	if err != nil {
		t.Error(err)
	}
	if eq, err := equals(current, createTestTable(t, pg)); !eq {
		t.Error("Current and original table different", err)
	}
}

// Add column, sync and verify
func TestSQLTable_Reconcile_2(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	table := createTestTable(t, pg)
	var err error
	if err = table.AddColumn(AddedColumn); err != nil {
		t.Error(err)
	}
	err = table.Reconcile()
	if err != nil {
		t.Error(err)
	}
	current := pg.makeTable(TableName)
	err = current.Sync()
	if err != nil {
		t.Error(err)
	}
	if eq, err := equals(current, table); !eq {
		t.Error("Current and original table different", err)
	}
}

// Revert to original definition (i.e. drop added column). Sync and verify
func TestSQLTable_Reconcile_3(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	table := createTestTable(t, pg)
	var err error
	err = table.Reconcile()
	if err != nil {
		t.Error(err)
	}
	current := pg.makeTable(TableName)
	err = current.Sync()
	if err != nil {
		t.Error(err)
	}
	if eq, err := equals(current, table); !eq {
		t.Error("Current and original table different", err)
	}
}

// Update default value
func TestSQLTable_Reconcile_4(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	table := createTestTable(t, pg)
	var err error
	var col = table.GetColumnByName("indexedcolumn")
	col.Default = "38"
	err = table.Reconcile()
	if err != nil {
		t.Error(err)
	}
	current := pg.makeTable(TableName)
	err = current.Sync()
	if err != nil {
		t.Error(err)
	}
	if eq, err := equals(current, table); !eq {
		t.Error("Current and original table different", err)
	}
}

func TestSQLTable_Drop(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	table := pg.makeTable(TableName)
	var exists bool
	var err error
	if exists, err = table.Exists(); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("Table to be dropped '%s' does not exist", TableName)
	}
	err = table.Drop()
	if err != nil {
		t.Fatal(err)
	}
	if exists, err = table.Exists(); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Errorf("Table to be dropped '%s' still exists", TableName)
	}
	bogus := pg.makeTable(BogusTable)
	err = bogus.Drop()
	if err != nil {
		t.Error(err)
	}
}

var TableName2 = "TestTable2"
var TableColumns2 = []SQLColumn{
	{Name: "primarykey1", SQLType: "text", Nullable: false, Default: "", PrimaryKey: false, Unique: true},
	{Name: "primarykey2", SQLType: "integer", Nullable: false, Default: "", PrimaryKey: false},
	{Name: "indexedcolumn", SQLType: "integer", Nullable: true, Default: "42", Indexed: true},
	{Name: "uniquecolumn", SQLType: "text", Nullable: false, Default: "''", Unique: true},
	{Name: "compoundindex_1", SQLType: "text", Nullable: false, Default: "''", Unique: false},
	{Name: "compoundindex_2", SQLType: "integer", Nullable: false, Default: "12", Unique: false},
}
var TableIndexes2 = []SQLIndex{
	{
		Columns:    []string{"primarykey1", "primarykey2"},
		PrimaryKey: true,
	},
	{
		Columns: []string{"uniquecolumn", "compoundindex_1"},
		Unique:  true,
	},
	{
		Columns: []string{"compoundindex_1", "compoundindex_2"},
		Unique:  false,
	},
}

func createTestTable2(t *testing.T, pg *PostgreSQLAdapter) SQLTable {
	return createTableByDef(t, pg, TableName2, TableColumns2, TableIndexes2)
}

// Verify that created table is identical to defined one
func TestSQLTable_Sync_2(t *testing.T) {
	pg := GetPostgreSQLAdapter()
	table := createTestTable2(t, pg)
	var exists bool
	var err error
	if exists, err = table.Exists(); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("Table '%s' already exists", TableName2)
	}
	if err = table.Reconcile(); err != nil {
		t.Fatalf("Could not reconcile table: %s", err)
	}
	current := pg.makeTable(TableName2)
	if exists, err = current.Exists(); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Errorf("Table '%s' does not exists", TableName2)
	}
	err = current.Sync()
	if err != nil {
		t.Error(err)
	}
	if eq, err := equals(current, table); !eq {
		t.Error("Current and original table different", err)
	}
}
