package cockroach

import (
	"fmt"

	"strings"

	"github.com/ellcrys/cocoon/core/common"
	"github.com/ellcrys/gorm"
	"github.com/ellcrys/patchain"
	"github.com/ellcrys/patchain/cockroach/tables"
	"github.com/ellcrys/util"
	"github.com/fatih/structs"
	"github.com/iancoleman/strcase"
	_ "github.com/jinzhu/gorm/dialects/postgres" // postgres dialect
	"github.com/ncodes/jsq"
	logging "github.com/op/go-logging"
	"github.com/pkg/errors"
)

// blacklistedFields cannot be included in JSQ query
var blacklistedFields = []string{"partition_id", "JSQ_params"}

// DB defines a structure that implements the DB interface
// to provide database access
type DB struct {
	db               *gorm.DB
	ConnectionString string
	log              *logging.Logger
	noLogging        bool
}

// NewDB creates a new DB db instance
func NewDB() (db *DB) {
	db = new(DB)
	db.log, _ = logging.GetLogger("patchain/cdb")
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

// GetValidObjectFields the json name of fields that can be queried using the JSQ parser.
func (c *DB) GetValidObjectFields() (fields []string) {
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
	return jsq.NewJSQ(c.GetValidObjectFields())
}

// GetLogger returns the package's logger
func (c *DB) GetLogger() *logging.Logger {
	return c.log
}

// NoLogging turns off logging for all log levels except CRITICAL logs
func (c *DB) NoLogging() {
	c.noLogging = true
	if c.log != nil {
		logging.SetLevel(logging.CRITICAL, c.log.Module)
	}
}

// Close closes the database connection
func (c *DB) Close() error {
	return c.db.Close()
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

// CreateTables creates the tables required if they do not exists.
// Returns nil if table already exists
func (c *DB) CreateTables() error {
	c.db.AutoMigrate(&tables.Object{})
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
	return dbTx.GetConn().(*gorm.DB).LogMode(!c.noLogging).Create(obj).Error
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

// UpdatePeerHash updates the peer hash of an object
func (c *DB) UpdatePeerHash(obj interface{}, newPeerHash string, options ...patchain.Option) error {
	dbTx, _ := c.getDBTxFromOption(options, &DB{db: c.db})
	return dbTx.GetConn().(*gorm.DB).LogMode(!c.noLogging).Model(obj).Update("peer_hash", newPeerHash).Error
}

// NewDB creates a new connection
func (c *DB) NewDB() patchain.DB {
	return &DB{db: c.db.NewScope(nil).NewDB()}
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
	err := dbTx.GetConn().(*gorm.DB).LogMode(!c.noLogging).Order("timestamp desc").Scopes(c.getQueryModifiers(q)...).Limit(1).Find(out).Error
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
	err := dbTx.GetConn().(*gorm.DB).LogMode(!c.noLogging).Scopes(c.getQueryModifiers(q)...).Find(out).Error
	if err != nil {
		if common.CompareErr(err, gorm.ErrRecordNotFound) == 0 {
			return patchain.ErrNotFound
		}
		return err
	}
	return nil
}

// Count returns a count of the number of documents that matches the query
func (c *DB) Count(q patchain.Query, out interface{}, options ...patchain.Option) error {
	dbTx, _ := c.getDBTxFromOption(options, &DB{db: c.db})
	return dbTx.GetConn().(*gorm.DB).
		LogMode(!c.noLogging).
		Scopes(c.getQueryModifiers(q)...).
		Model(q).
		Count(out).Error
}

// getQueryModifiers applies the query parameters
// associated with query object to the db connection.
func (c *DB) getQueryModifiers(q patchain.Query) []func(*gorm.DB) *gorm.DB {

	var modifiers []func(*gorm.DB) *gorm.DB
	qp := q.GetQueryParams()

	// if no expression in query param, use the query from the parameter
	if qp.Expr.Expr == "" {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Where(q)
		})
	} else {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Where(qp.Expr.Expr, qp.Expr.Args...)
		})
	}

	// get query params. If none, return modifier
	if qp == nil {
		return modifiers
	}

	if len(qp.KeyStartsWith) > 0 {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Where("key LIKE ?", qp.KeyStartsWith+"%")
		})
	}

	if len(qp.OrderBy) > 0 {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Order(qp.OrderBy)
		})
	}

	if qp.Limit > 0 {
		modifiers = append(modifiers, func(conn *gorm.DB) *gorm.DB {
			return conn.Limit(qp.Limit)
		})
	}

	return modifiers
}
