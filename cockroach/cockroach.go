package cockroach

import (
	"fmt"

	"strings"

	"github.com/ellcrys/util"
	"github.com/fatih/structs"
	"github.com/iancoleman/strcase"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres" // postgres dialect
	"github.com/jinzhu/inflection"
	"github.com/ncodes/cocoon/core/common"
	"github.com/ncodes/jsq"
	"github.com/ncodes/patchain"
	"github.com/ncodes/patchain/cockroach/tables"
	logging "github.com/op/go-logging"
	"github.com/pkg/errors"
)

// blacklistedFields cannot be included in JSQ query
var blacklistedFields = []string{"creator_id", "partition_id", "JSQ_params", "schema_version"}

// DB defines a structure that implements the DB interface
// to provide database access
type DB struct {
	db               *gorm.DB
	ConnectionString string
	log              *logging.Logger
}

// NewDB creates a new DB db instance
func NewDB() (_db *DB) {
	_db = new(DB)
	_db.log, _ = logging.GetLogger("patchain/cdb")
	return
}

// Connect connects to a database and ledgers a reference in the object
func (c *DB) Connect(maxOpenConn, maxIdleConn int) error {

	_db, err := gorm.Open("postgres", c.ConnectionString)
	if err != nil {
		return errors.Wrap(err, "failed to connect to cockroach")
	}

	_db.DB().SetMaxIdleConns(maxIdleConn)
	_db.DB().SetMaxOpenConns(maxOpenConn)

	c.db = _db
	c.log.Info("Successfully connected to a cockroach db")
	return nil
}

// getValidObjectFields from the tables.Object. JSON tag must be set.
func (c *DB) getValidObjectFields() (fields []string) {
	var fieldNames = structs.New(tables.Object{}).Fields()
	for _, f := range fieldNames {
		field := strcase.ToSnake(f.Tag("json"))
		field = strings.Split(field, ",")[0]
		if !util.InStringSlice(blacklistedFields, field) {
			fields = append(fields, field)
		}
	}
	return
}

// NewQuery creates an instance of a json structured query parser
func (c *DB) NewQuery() jsq.Query {
	return jsq.NewJSQ(c.getValidObjectFields())
}

// GetLogger returns the package's logger
func (c *DB) GetLogger() *logging.Logger {
	return c.log
}

// NoLogging turns off logging for all log levels except CRITICAL logs
func (c *DB) NoLogging() {
	if c.log != nil {
		logging.SetLevel(logging.CRITICAL, c.log.Module)
	}
}

// Close closes the database connection
func (c *DB) Close() error {
	return c.Close()
}

// GetConn returns the underlying db connection
func (c *DB) GetConn() interface{} {
	return c.db
}

// SetConn sets the underlying database connection to use
func (c *DB) SetConn(conn interface{}) error {
	switch _conn := conn.(type) {
	case *gorm.DB:
		c.db = _conn
	default:
		return fmt.Errorf("connection type not supported. Requires *gorm.DB")
	}
	return nil
}

// hasTable checks whether a table exists in the database.
// Note: I could have used c.db.hasTable, but it turns out
// postgres driver's implementation doesn't work with cockroach db
func (c *DB) hasTable(tbl interface{}) bool {
	var count int
	c.db.CommonDB().QueryRow(
		"SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE table_name = $1 AND table_schema = $2",
		inflection.Plural(gorm.ToDBName(structs.New(tbl).Name())),
		c.db.Dialect().CurrentDatabase()).Scan(&count)
	return count > 0
}

// createTableIfNotExist if table does not exists. It will also add
// new columns if not existing in the current table
func (c *DB) createTableIfNotExist(tbl interface{}) error {
	if c.hasTable(tbl) {
		return nil
	}
	return c.db.CreateTable(tbl).Error
}

// CreateTables creates the tables required if they do not exists.
// Returns nil if table already exists
func (c *DB) CreateTables() error {

	// create object table
	if err := c.createTableIfNotExist(&tables.Object{}); err != nil {
		return errors.Wrap(err, "failed to create/modify object table")
	}

	return nil
}

// getDBTxFromOption gets the db added in the slice of options.
// Returns the fallback connection if no UseDBOption is found.
func (c *DB) getDBTxFromOption(options []patchain.Option, fallback patchain.DB) (patchain.DB, bool) {
	var finish bool
	var dbTx patchain.DB

	if len(options) > 0 {
		for _, option := range options {
			if option.GetName() == patchain.UseDBOptionName {
				dbTx = option.GetValue().(patchain.DB)
				finish = option.(*patchain.UseDBOption).Finish
				break
			}
		}
	}
	if dbTx == nil {
		dbTx = fallback
	}
	return dbTx, finish
}

// Create creates a new record
func (c *DB) Create(obj interface{}, options ...patchain.Option) error {
	dbTx, _ := c.getDBTxFromOption(options, &DB{db: c.db})
	return dbTx.GetConn().(*gorm.DB).Create(obj).Error
}

// CreateBulk creates more than one objects in a single transaction.
func (c *DB) CreateBulk(objs []interface{}, options ...patchain.Option) error {
	for _, obj := range objs {
		if err := c.Create(obj, options...); err != nil {
			return err
		}
	}
	return nil
}

// Begin returns a database object with an active transaction session
func (c *DB) Begin() patchain.DB {
	return &DB{db: c.db.NewScope(nil).DB().Begin()}
}

// Transact starts a transaction. It returns a CommitFunc and a RollbackFunc for
// committing and rolling back the transaction respectively
func (c *DB) Transact(txF patchain.TxFunc) error {
	return c.TransactWithDB(c.Begin(), true, txF)
}

// TransactWithDB is the same as Begin but it takes a database connection with an active session and calls the transaction
// function passing the connection to it. If finishTx is set to true and the transaction has not been committed or rolled back,
// the transaction will be committed if the function returns nil or rolled back if it returns an error.
func (c *DB) TransactWithDB(txDb patchain.DB, finishTx bool, txF patchain.TxFunc) error {
	var committed, rolledBack = false, false
	err := txF(txDb, func() error {
		committed = true
		return txDb.Commit()
	}, func() error {
		rolledBack = true
		return txDb.Rollback()
	})
	if finishTx && !committed && !rolledBack {
		if err != nil {
			if rollbackErr := txDb.Rollback(); rollbackErr != nil {
				return errors.Wrap(rollbackErr, "failed to rollback")
			}
			return err
		}
		if commitErr := txDb.Commit(); commitErr != nil {
			return errors.Wrap(commitErr, "failed to commit")
		}
	}
	return err
}

// Commit commits the active session in the db connection
func (c *DB) Commit() error {
	return c.db.Commit().Error
}

// Rollback rolls back the active session in the db connection
func (c *DB) Rollback() error {
	return c.db.Rollback().Error
}

// GetLast gets the last document that matches the query object
func (c *DB) GetLast(q patchain.Query, out interface{}, options ...patchain.Option) error {
	dbTx, _ := c.getDBTxFromOption(options, &DB{db: c.db})
	err := dbTx.GetConn().(*gorm.DB).
		Scopes(c.getQueryModifiers(q)...).
		Last(out).Error
	if err != nil {
		if common.CompareErr(err, gorm.ErrRecordNotFound) == 0 {
			return patchain.ErrNotFound
		}
		return err
	}
	return nil
}

// GetAll fetches all documents that match a query
func (c *DB) GetAll(q patchain.Query, out interface{}, options ...patchain.Option) error {
	dbTx, _ := c.getDBTxFromOption(options, &DB{db: c.db})
	return dbTx.GetConn().(*gorm.DB).
		Scopes(c.getQueryModifiers(q)...).
		Find(out).Error
}

// Count returns a count of the number of documents that matches the query
func (c *DB) Count(q patchain.Query, out *int64, options ...patchain.Option) error {
	dbTx, _ := c.getDBTxFromOption(options, &DB{db: c.db})
	return dbTx.GetConn().(*gorm.DB).
		Scopes(c.getQueryModifiers(q)...).
		Model(q).
		Count(out).Error
}

// getQueryModifiers applies the query parameters associated with query object to the db connection
func (c *DB) getQueryModifiers(q patchain.Query) []func(*gorm.DB) *gorm.DB {
	var modifiers []func(*gorm.DB) *gorm.DB

	// add query modifier
	modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
		return conn.Where(q)
	})

	// get query params. If none, return modifier
	qp := q.GetQueryParams()
	if qp == nil {
		return modifiers
	}

	if len(qp.KeyStartsWith) > 0 {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Where("key LIKE ?", qp.KeyStartsWith+"%")
		})
	}

	if qp.MustOrderByTimestampDesc {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Order("timestamp desc")
		})
	}

	// orderer by timestamp if none is set
	if len(qp.OrderBy) > 0 {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Order(qp.OrderBy)
		})
	} else {
		if !qp.MustOrderByTimestampDesc {
			modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
				return conn.Order("timestamp desc")
			})
		}
	}

	return modifiers
}
