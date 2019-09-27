package grumble

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
)

type PostgreSQLAdapter struct {
	Hostname      string
	Username      string
	Password      string
	AdminUser     string
	AdminPassword string
	DatabaseName  string
	Schema        string
	WipeDatabase  bool
	WipeSchema    bool
	DatabaseInit  string
	SchemaInit    string
	Reconcile     bool
	conn          map[bool]*sql.DB
	tx            map[*sql.DB]*sql.Tx
}

var once sync.Once
var defaultAdapter = PostgreSQLAdapter{
	Hostname:      "localhost",
	Username:      "grumble",
	AdminUser:     "postgres",
	Password:      "secret",
	AdminPassword: "evenmoresecret",
	DatabaseName:  "grumble",
	Schema:        "grumble",
	WipeDatabase:  false,
	WipeSchema:    false,
	DatabaseInit:  "",
	SchemaInit:    "",
	Reconcile:     true,
}

var adapter *PostgreSQLAdapter

type SQLColumn struct {
	Name       string
	SQLType    string
	Default    string
	Nullable   bool
	PrimaryKey bool
	Unique     bool
	Indexed    bool
}

type SQLIndex struct {
	Name       string
	Columns    []string
	PrimaryKey bool
	Unique     bool
}

type SQLTable struct {
	pg            PostgreSQLAdapter
	Schema        string
	TableName     string
	Columns       []SQLColumn
	Indexes       []SQLIndex
	columnsByName map[string]int
	indexesByName map[string]int
}

type SQLTemplate struct {
	Name     string
	SQL      string
	Template *template.Template
}

func (tmpl SQLTemplate) tweak(txt string) string {
	re := regexp.MustCompile("__(count|reset)(:[[:digit:]]+)?__")
	ix := int64(1)
	arg := int64(-1)
	for m := re.FindStringSubmatchIndex(txt); m != nil; m = re.FindStringSubmatchIndex(txt) {
		if m[4] >= 0 {
			arg, _ = strconv.ParseInt(txt[m[4]+1:m[5]], 0, 0)
		}
		repl := ""
		switch txt[m[2]:m[3]] {
		case "reset":
			if arg >= 0 {
				ix = arg
			}
		case "count":
			if arg >= 0 {
				ix = arg
			}
			repl = fmt.Sprintf("$%d", ix)
			ix += 1
		default:
			repl = ""
		}
		switch {
		case m[0] == 0 && m[1] == len(txt):
			txt = repl
		case m[0] == 0:
			txt = fmt.Sprintf("%s%s", repl, txt[m[1]:])
		case m[1] == len(txt):
			txt = fmt.Sprintf("%s%s", txt[:m[0]], repl)
		default:
			txt = fmt.Sprintf("%s%s%s", txt[:m[0]], repl, txt[m[1]:])
		}
	}
	return txt
}

func (tmpl SQLTemplate) Process(data interface{}) (sql string, err error) {
	if tmpl.Template == nil {
		tmpl.Template = template.Must(template.New(tmpl.Name).Parse(tmpl.SQL))
	}
	var buf bytes.Buffer
	if err = tmpl.Template.Execute(&buf, data); err != nil {
		return
	}
	sql = tmpl.tweak(string(buf.Bytes()))
	return
}

func (tmpl SQLTemplate) Exec(conn *sql.DB, data interface{}, values ...interface{}) (err error) {
	s, err := tmpl.Process(data)
	if err != nil {
		return
	}
	_, err = conn.Exec(s, values...)
	return
}

func GetPostgreSQLAdapter() *PostgreSQLAdapter {
	once.Do(func() {
		adapter = &defaultAdapter
		var err error

		var jsonText []byte
		if jsonText, err = ioutil.ReadFile("conf/database.conf"); err != nil {
			return
		}
		adapter = new(PostgreSQLAdapter)
		err = json.Unmarshal(jsonText, adapter)
		if err != nil {
			log.Printf("Error decoding database.conf: %s\n", err.Error())
			adapter = &defaultAdapter
		}
		adapter.conn = make(map[bool]*sql.DB)
		adapter.tx = make(map[*sql.DB]*sql.Tx)
		adapter.initialize()
	})
	ret := new(PostgreSQLAdapter)
	*ret = *adapter
	ret.conn = make(map[bool]*sql.DB)
	ret.tx = make(map[*sql.DB]*sql.Tx)
	return ret
}

func (pg *PostgreSQLAdapter) GetConnection() *sql.DB {
	return pg.getConnection(false)
}

func (pg *PostgreSQLAdapter) GetAdminConnection() *sql.DB {
	return pg.getConnection(true)
}

func (pg *PostgreSQLAdapter) getConnection(admin bool) *sql.DB {
	if pg.conn[admin] != nil {
		return pg.conn[admin]
	}
	var user, pwd string
	if admin {
		user = pg.AdminUser
		pwd = pg.AdminPassword
	} else {
		user = pg.Username
		pwd = pg.Password
	}
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable",
		user, pwd, pg.DatabaseName, pg.Hostname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	pg.conn[admin] = db
	return db
}

func (pg *PostgreSQLAdapter) Close() {
	pg.CloseConnection(false)
}

func (pg *PostgreSQLAdapter) CloseConnection(admin bool) {
	if err := pg.CommitTX(admin); err != nil {
		log.Printf("Error committing transaction: %q", err)
	}
	if pg.conn[admin] != nil {
		if err := pg.conn[admin].Close(); err != nil {
			log.Fatal(err)
		}
		delete(pg.conn, admin)
	}
}

func (pg *PostgreSQLAdapter) Begin() (err error) {
	return pg.BeginTX(false)
}

func (pg *PostgreSQLAdapter) BeginTX(admin bool) (err error) {
	conn := pg.getConnection(admin)
	tx := pg.tx[conn]
	if tx != nil {
		return
	}
	if tx, err = conn.Begin(); err != nil {
		return
	}
	pg.tx[conn] = tx
	return
}

func (pg *PostgreSQLAdapter) Commit() (err error) {
	return pg.CommitTX(false)
}

func (pg *PostgreSQLAdapter) CommitTX(admin bool) (err error) {
	conn := pg.getConnection(admin)
	tx := pg.tx[conn]
	if tx == nil {
		return
	}
	err = tx.Commit()
	delete(pg.tx, conn)
	return
}

func (pg *PostgreSQLAdapter) Rollback() (err error) {
	return pg.RollbackTX(false)
}

func (pg *PostgreSQLAdapter) RollbackTX(admin bool) (err error) {
	conn := pg.getConnection(admin)
	tx := pg.tx[conn]
	if tx == nil {
		return
	}
	err = tx.Rollback()
	delete(pg.tx, conn)
	return
}

func (pg PostgreSQLAdapter) Work(work func(*sql.DB) error) (ret error) {
	return pg.doWork(false, work)
}

func (pg PostgreSQLAdapter) doWork(admin bool, work func(*sql.DB) error) (ret error) {
	conn, leaveOpen := pg.conn[admin]
	defer func() {
		if !leaveOpen {
			pg.CloseConnection(admin)
		}
	}()
	if !leaveOpen {
		conn = pg.getConnection(admin)
	}
	ret = work(conn)
	return
}

func (pg PostgreSQLAdapter) TX(work func(*sql.DB) error) (ret error) {
	return pg.runTX(false, work)
}

func (pg PostgreSQLAdapter) runTX(admin bool, work func(*sql.DB) error) (err error) {
	return pg.doWork(admin, func(db *sql.DB) (err error) {
		if _, txActive := pg.tx[db]; txActive {
			err = work(db)
		} else {
			if err = pg.BeginTX(admin); err == nil {
				defer func() {
					if err == nil {
						err = pg.CommitTX(admin)
					} else {
						if e := pg.RollbackTX(admin); e != nil {
							// FIXME Wrap err in e
							log.Printf("Error rolling back transaction: '%s'", e)
						}
					}
				}()
				err = work(db)
			}
		}
		return
	})
}

func (pg PostgreSQLAdapter) runSQLFile(conn *sql.DB, sqlFile string) (err error) {
	err = nil
	if sqlFile != "" {
		var templateText []byte
		if templateText, err = ioutil.ReadFile(sqlFile); err != nil {
			return
		}
		txt := string(templateText)
		if txt != "" {
			var tmpl *template.Template
			tmpl, err = template.New(sqlFile).Parse(txt)
			if err != nil {
				return
			}
			var buf bytes.Buffer
			if err = tmpl.Execute(&buf, pg); err != nil {
				return
			}
			if _, err = conn.Exec(string(buf.Bytes())); err != nil {
				return
			}
		}
	}
	return
}

func (pg PostgreSQLAdapter) initialize() {
	if err := pg.runTX(true, pg.resetDatabase); err != nil {
		panic(err)
	}
}

func (pg PostgreSQLAdapter) resetDatabase(conn *sql.DB) (err error) {
	createDb := false
	dropSchema := pg.WipeSchema
	if pg.DatabaseName != "postgres" && pg.WipeDatabase {
		if _, err = conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS \"%s\"", pg.DatabaseName)); err != nil {
			return
		}
		createDb = true
	} else {
		var count int
		rows := conn.QueryRow("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = $1", pg.DatabaseName)
		err = rows.Scan(&count)
		switch {
		case err != nil:
			return
		case count == 0:
			createDb = true
		case count == 1:
			createDb = false
		}
	}
	if createDb {
		if _, err = conn.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", pg.DatabaseName)); err != nil {
			return
		}
		dropSchema = false
		if err = pg.runSQLFile(conn, pg.DatabaseInit); err != nil {
			return
		}
	}
	return pg.resetSchema(dropSchema, conn)
}

func (pg PostgreSQLAdapter) resetSchema(dropSchema bool, conn *sql.DB) (err error) {
	err = nil
	if pg.Schema != "" {
		createSchema := false
		if dropSchema {
			if _, err = conn.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS \"%s\" CASCADE", pg.Schema)); err != nil {
				return
			}
			createSchema = true
		} else {
			var row *sql.Row
			row = conn.QueryRow("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = $1", pg.Schema)
			var count int
			err = row.Scan(&count)
			switch {
			case err != nil:
				return
			case count == 1:
				createSchema = false
			case count == 0:
				createSchema = true
			}
		}
		if createSchema {
			if _, err = conn.Exec(fmt.Sprintf("CREATE SCHEMA \"%s\" AUTHORIZATION %s", pg.Schema, pg.Username)); err != nil {
				return
			}
			if err = pg.runSQLFile(conn, pg.SchemaInit); err != nil {
				return
			}
		}
	}
	return
}

func (pg PostgreSQLAdapter) GetSchema() string {
	ret := pg.Schema
	if ret == "" {
		ret = "public"
	}
	return ret
}

func (pg PostgreSQLAdapter) ResetSchema() (err error) {
	return pg.runTX(true, func(db *sql.DB) error {
		return pg.resetSchema(true, db)
	})
}

func (pg PostgreSQLAdapter) makeTable(tableName string) SQLTable {
	ret := SQLTable{pg: pg, TableName: tableName, Schema: pg.GetSchema()}
	ret.Columns = make([]SQLColumn, 0)
	ret.Indexes = make([]SQLIndex, 0)
	ret.columnsByName = make(map[string]int)
	ret.indexesByName = make(map[string]int)
	return ret
}

func (table SQLTable) QualifiedName() string {
	return fmt.Sprintf("%q.%q", table.Schema, table.TableName)
}

func (table SQLTable) exists(conn *sql.DB) (result bool, err error) {
	sqlCmd := "SELECT table_name FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2"
	var dummy string
	err = conn.QueryRow(sqlCmd, table.TableName, table.Schema).Scan(&dummy)
	switch {
	case err == sql.ErrNoRows:
		result = false
		err = nil
	case err != nil:
		return
	default:
		result = true
	}
	return
}

func (table SQLTable) Exists() (result bool, err error) {
	result = false
	err = table.pg.TX(func(conn *sql.DB) (err error) {
		result, err = table.exists(conn)
		return
	})
	return
}

func (table SQLTable) GetColumnByName(columnName string) (column *SQLColumn) {
	var found bool
	var ix int
	column = nil
	ix, found = table.columnsByName[columnName]
	if found {
		column = &table.Columns[ix]
	}
	return
}

func (table SQLTable) GetIndexByName(indexName string) (index *SQLIndex) {
	var found bool
	var ix int
	index = nil
	ix, found = table.indexesByName[indexName]
	if found {
		index = &table.Indexes[ix]
	}
	return
}

func (table *SQLTable) syncIndexes(conn *sql.DB) (err error) {
	s := `WITH indexData AS (
    SELECT c.oid AS tableoid, c.relname AS tablename, i.relname AS indexname, 
           x.indnatts, x.indkey, x.indisunique as isunique,
           generate_subscripts(x.indkey, 1) AS ix
    FROM pg_index x
         JOIN pg_class c ON c.oid = x.indrelid
         JOIN pg_class i ON i.oid = x.indexrelid
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE i.relkind = 'i'::"char" AND n.nspname = $1
)
SELECT idx.indexname, array_agg(attr.attname) as columns, bool_and(idx.isunique)
    FROM indexData idx, pg_attribute attr
    WHERE attr.attrelid = idx.tableoid AND attr.attnum = idx.indkey[idx.ix] AND idx.tablename = $2
    GROUP BY idx.tablename, idx.indexname`
	var rows *sql.Rows
	rows, err = conn.Query(s, table.Schema, table.TableName)
	if err != nil {
		return
	} else {
		for rows.Next() {
			var indexName string
			var columns []string
			var unique bool
			if err = rows.Scan(&indexName, pq.Array(&columns), &unique); err != nil {
				return
			}
			if len(columns) == 1 {
				column := table.GetColumnByName(columns[0])
				if unique {
					column.Unique = true
				} else {
					column.Indexed = true
				}
			} else {
				var index SQLIndex
				index.Name = indexName
				index.Columns = columns
				index.Unique = unique
				table.Indexes = append(table.Indexes, index)
				table.indexesByName[index.Name] = len(table.Indexes) - 1
			}
		}
	}
	return
}

func (table *SQLTable) syncConstraints(conn *sql.DB) (err error) {
	s := `SELECT array_agg(cu.column_name)::text, tc.constraint_name, max(tc.constraint_type) 
				FROM information_schema.constraint_column_usage cu  
				INNER JOIN information_schema.table_constraints tc USING (constraint_schema, constraint_name)
				WHERE cu.table_schema = $1 AND cu.table_name = $2 GROUP BY tc.constraint_name`
	var rows *sql.Rows
	rows, err = conn.Query(s, table.Schema, table.TableName)
	if err != nil {
		return
	} else {
		type constraint struct {
			name    string
			columns []string
			typ     string
		}
		var constraints = make(map[string]*constraint)
		for rows.Next() {
			var constr constraint
			if err = rows.Scan(pq.Array(&constr.columns), &constr.name, &constr.typ); err != nil {
				return
			}
			constraints[constr.name] = &constr
		}
		for _, constr := range constraints {
			if len(constr.columns) == 1 {
				var column = table.GetColumnByName(constr.columns[0])
				if column != nil {
					switch constr.typ {
					case "UNIQUE":
						column.Unique = true
						column.Indexed = false
						column.PrimaryKey = false
					case "PRIMARY KEY":
						column.Unique = true
						column.Indexed = false
						column.PrimaryKey = true
					}
				}
			} else {
				index := table.GetIndexByName(constr.name)
				if index != nil {
					switch constr.typ {
					case "UNIQUE":
						index.Unique = true
						index.PrimaryKey = false
					case "PRIMARY KEY":
						index.Unique = false
						index.PrimaryKey = true
					}
				}
			}
		}
	}
	return
}

func (table *SQLTable) syncColumns(conn *sql.DB) (err error) {
	s := `SELECT column_name, column_default, is_nullable, data_type 
				FROM information_schema.columns 
				WHERE table_schema = $1 AND table_name = $2`
	var rows *sql.Rows
	rows, err = conn.Query(s, table.Schema, table.TableName)
	if err != nil {
		return
	} else {
		for rows.Next() {
			var col = SQLColumn{}
			var nullable string
			var columnDefault sql.NullString
			if err = rows.Scan(&col.Name, &columnDefault, &nullable, &col.SQLType); err != nil {
				return
			}
			if columnDefault.Valid {
				if strings.Index(columnDefault.String, "nextval") == 0 && col.SQLType == "integer" {
					col.SQLType = "serial"
				} else {
					col.Default = columnDefault.String
					ix := strings.Index(col.Default, "::")
					if ix >= 0 {
						col.Default = col.Default[:ix]
					}
				}
			} else {
				col.Default = ""
			}
			col.Nullable = nullable == "YES"
			table.Columns = append(table.Columns, col)
			table.columnsByName[col.Name] = len(table.Columns) - 1
		}
	}
	return
}

func (table *SQLTable) Sync() (err error) {
	return table.pg.TX(func(conn *sql.DB) (err error) {
		table.Columns = make([]SQLColumn, 0)
		table.Indexes = make([]SQLIndex, 0)
		table.columnsByName = make(map[string]int)
		table.indexesByName = make(map[string]int)
		var exists bool
		if exists, err = table.exists(conn); (err != nil) || !exists {
			return
		}
		if err = table.syncColumns(conn); err != nil {
			return
		}
		if err = table.syncIndexes(conn); err != nil {
			return
		}
		if err = table.syncConstraints(conn); err != nil {
			return
		}
		return
	})
}

func (table *SQLTable) AddColumn(column SQLColumn) (err error) {
	if _, found := table.columnsByName[column.Name]; found {
		err = errors.New(fmt.Sprintf("cannot add duplicate column '%s'", column.Name))
		return
	}
	if column.PrimaryKey {
		for _, i := range table.Indexes {
			if i.PrimaryKey {
				err = errors.New(fmt.Sprintf("cannot add primary key column '%s' in addition to current multi-column PK",
					column.Name))
				return
			}
		}
		for _, c := range table.Columns {
			if c.PrimaryKey {
				err = errors.New(fmt.Sprintf("cannot add second primary key column '%s' in addition to current PK '%s'",
					column.Name, c.Name))
				return
			}
		}
	}
	table.Columns = append(table.Columns, column)
	table.columnsByName[column.Name] = len(table.Columns) - 1
	return
}

func (table *SQLTable) AddIndex(index SQLIndex) (err error) {
	if index.Name != "" {
		if _, found := table.indexesByName[index.Name]; found {
			err = errors.New(fmt.Sprintf("cannot add duplicate index '%s'", index.Name))
			return
		}
	}
	if index.PrimaryKey {
		for _, c := range table.Columns {
			if c.PrimaryKey {
				err = errors.New(fmt.Sprintf("cannot set multi-column PK. Current dedicated PK column is '%s'",
					c.Name))
				return
			}
		}
	}
	if len(index.Columns) == 0 {
		err = errors.New("cannot add index without columns")
		return
	}
	for _, columnName := range index.Columns {
		if _, found := table.columnsByName[columnName]; !found {
			err = errors.New(fmt.Sprintf("cannot add index '%s' with not-existent key column '%s'",
				index.Name, columnName))
			return
		}
	}
	if len(index.Columns) == 1 {
		c := table.GetColumnByName(index.Columns[0])
		if index.PrimaryKey {
			c.Indexed = false
			c.Unique = false
			c.Nullable = false
			c.PrimaryKey = true
		} else {
			c.Indexed = true
		}
	} else {
		if index.Name == "" {
			index.Name = table.TableName + "_" + strings.Join(index.Columns, "_")
		}
		table.Indexes = append(table.Indexes, index)
		table.indexesByName[index.Name] = len(table.Indexes) - 1
		for _, columnName := range index.Columns {
			column := table.GetColumnByName(columnName)
			column.Nullable = false
		}
	}
	return
}

var createTable = SQLTemplate{Name: "CreateTable", SQL: `{{$Table := .TableName}}{{$Qualified := .QualifiedName}}
{{define "indexcolumns"}}({{range $i, $col := .Columns}}{{if gt $i 0}}, {{end}}"{{$col}}"{{end}}){{end}}
CREATE TABLE {{$Qualified}} (
  {{range $i, $c := .Columns}}
    {{if gt $i 0}},{{end}}"{{$c.Name}}" {{$c.SQLType}}{{$l := len $c.Default}}{{if gt $l 0}} DEFAULT {{$c.Default}}{{end}}{{if not $c.Nullable}} NOT NULL{{end}}{{if $c.Unique}} UNIQUE{{end}}{{if $c.PrimaryKey}} PRIMARY KEY{{end}}
  {{end}}
  {{range .Indexes}}
    {{if .PrimaryKey}}, CONSTRAINT "{{.Name}}" PRIMARY KEY {{template "indexcolumns" .}}{{end}}
  {{end}}
);
  {{range $c := .Columns}}
	{{if $c.Indexed}}
CREATE INDEX "{{$Table}}_{{$c.Name}}" ON {{$Qualified}} ("{{$c.Name}}");
    {{end}}
  {{end}}
  {{range .Indexes}}
{{if not .PrimaryKey}}CREATE{{if .Unique}} UNIQUE{{end}} INDEX "{{.Name}}" ON {{$Qualified}} {{template "indexcolumns" .}}{{end}};
  {{end}} 
`}

func (table SQLTable) create(conn *sql.DB) (err error) {
	return createTable.Exec(conn, table)
}

func (table SQLTable) alterAddColumn(conn *sql.DB, column SQLColumn) (err error) {
	s := fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s",
		table.QualifiedName(), column.Name, column.SQLType)
	if !column.Nullable {
		s += " NOT NULL"
	}
	if column.Default != "" {
		s += fmt.Sprintf(" DEFAULT %s", column.Default)
	}
	if column.PrimaryKey {
		s += " PRIMARY KEY"
	}
	if column.Unique {
		s += " UNIQUE"
	}
	if _, err = conn.Exec(s); err != nil {
		return
	}
	if column.Indexed && !column.PrimaryKey {
		s = fmt.Sprintf("CREATE INDEX \"%s_%s\" on %s ( \"%s\" )",
			table.TableName, column.Name, table.QualifiedName(), column.Name)
		if _, err = conn.Exec(s); err != nil {
			return
		}
	}
	return
}

func (table SQLTable) alterDropColumn(conn *sql.DB, column SQLColumn) (err error) {
	if column.Indexed {
		if err = table.alterAddColumnIndex(conn, column); err != nil {
			return
		}
	}
	s := fmt.Sprintf("ALTER TABLE %s DROP COLUMN \"%s\"", table.QualifiedName(), column.Name)
	_, err = conn.Exec(s)
	return
}

func (table SQLTable) alterAddColumnIndex(conn *sql.DB, column SQLColumn) (err error) {
	var unique string
	if column.Unique {
		unique = "UNIQUE "
	}
	s := fmt.Sprintf("CREATE %sINDEX \"%s_%s\" ON %s (\"%s\")",
		unique, table.TableName, column.Name, table.QualifiedName(), column.Name)
	_, err = conn.Exec(s)
	return
}

func (table SQLTable) alterDropColumnIndex(conn *sql.DB, column string) (err error) {
	s := fmt.Sprintf("DROP INDEX \"%s_%s\"", table.TableName, column)
	_, err = conn.Exec(s)
	return
}

func (table SQLTable) alterCreateIndex(conn *sql.DB, index SQLIndex) (err error) {
	var unique string
	if index.Unique {
		unique = "UNIQUE "
	}
	s := fmt.Sprintf("CREATE %sINDEX \"%s\" ON %s (\"%s\")",
		unique, index.Name, table.QualifiedName(), strings.Join(index.Columns, "\", \""))
	_, err = conn.Exec(s)
	return
}

func (table SQLTable) alterDropIndex(conn *sql.DB, index string) (err error) {
	s := fmt.Sprintf("DROP INDEX \"%s\"", index)
	_, err = conn.Exec(s)
	return
}

func (table SQLTable) reconcileColumn(conn *sql.DB, newColumn SQLColumn, oldColumn *SQLColumn) (err error) {
	// HACK
	if oldColumn.SQLType == "ARRAY" || oldColumn.SQLType == "USER-DEFINED" {
		return
	}
	if newColumn.SQLType != oldColumn.SQLType {
		if err = table.alterDropColumn(conn, newColumn); err != nil {
			return
		}
		if err = table.alterAddColumn(conn, newColumn); err != nil {
			return
		}
		return
	}
	if newColumn.Indexed && !oldColumn.Indexed {
		if err = table.alterAddColumnIndex(conn, newColumn); err != nil {
			return
		}
	} else if !newColumn.Indexed && oldColumn.Indexed {
		if err = table.alterDropColumnIndex(conn, newColumn.Name); err != nil {
			return
		}
	}
	var alter string
	if newColumn.Default != oldColumn.Default {
		if newColumn.Default != "" {
			alter = fmt.Sprintf("SET DEFAULT %s", newColumn.Default)
		} else {
			alter = "DROP DEFAULT"
		}
	}
	if newColumn.Nullable != oldColumn.Nullable {
		if newColumn.Nullable {
			alter += " DROP"
		} else {
			alter += " SET"
		}
		alter += " NOT NULL"
	}
	if alter != "" {
		alter = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN \"%s\" %s", table.QualifiedName(), newColumn.Name, alter)
		_, err = conn.Exec(alter)
	}
	return
}

func (table SQLTable) Reconcile() (err error) {
	err = table.pg.TX(func(conn *sql.DB) (err error) {
		var current = table.pg.makeTable(table.TableName)
		current.Schema = table.Schema
		var exists bool
		if exists, err = current.exists(conn); err != nil {
			return
		}
		if !exists {
			err = table.create(conn)
			return
		}
		if !table.pg.Reconcile {
			return
		}
		if err = current.Sync(); err != nil {
			return
		}

		// Loop new columns. Create any that don't exist yet, reconcile existing ones.
		for _, newCol := range table.Columns {
			oldCol := current.GetColumnByName(newCol.Name)
			if oldCol == nil {
				// Column is new. Create:
				if err = table.alterAddColumn(conn, newCol); err != nil {
					return
				}
			} else {
				// Column exists. Reconcile:
				if err = table.reconcileColumn(conn, newCol, oldCol); err != nil {
					return
				}
			}
		}

		// Loop old columns. Drop any that shouldn't exist anymore:
		for _, oldCol := range current.Columns {
			newCol := table.GetColumnByName(oldCol.Name)
			if newCol == nil {
				// Doesn't exist anymore. Drop:
				if err = table.alterDropColumn(conn, oldCol); err != nil {
					return
				}
			}
		}

		// Loop new indexes. Create any that don't exist yet, reconcile existing ones.
		for _, newIndex := range table.Indexes {
			oldIndex := current.GetIndexByName(newIndex.Name)
			if oldIndex == nil {
				// Index is new. Create:
				if err = table.alterCreateIndex(conn, newIndex); err != nil {
					return
				}
			}
			// No reconciliation for changes in index def.
		}

		// Loop old indexes. Drop any that shouldn't exist anymore:
		for _, oldIndex := range current.Indexes {
			newIndex := table.GetIndexByName(oldIndex.Name)
			if newIndex == nil {
				// Doesn't exist anymore. Drop:
				if err = table.alterDropIndex(conn, oldIndex.Name); err != nil {
					return
				}
			}
		}
		return
	})
	return
}

func (table SQLTable) Drop() (err error) {
	return table.pg.TX(func(db *sql.DB) (err error) {
		s := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", table.QualifiedName())
		_, err = table.pg.GetConnection().Exec(s)
		return
	})
}
