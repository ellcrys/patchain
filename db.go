package patchain

import (
	"fmt"

	"github.com/ncodes/jsq"
)

var (
	// ErrNotFound indicates a missing data
	ErrNotFound = fmt.Errorf("not found")
)

// DB defines an interface for database operations
type DB interface {

	// Connect connects to the database returing an error if it failed
	Connect(maxOpenConn, maxIdleConn int) error

	// GetConn returns the database connection or session for use by external services.
	GetConn() interface{}

	// SetConn sets the connection or session
	SetConn(interface{}) error

	// NewDB create a new DB connection
	NewDB() DB

	// CreateTables creates the tables required for the patchain model.
	CreateTables() error

	// Create creates an object and adds it the the patchain table
	Create(obj interface{}, options ...Option) error

	// CreateBulk is like Create but supports multiple objects
	CreateBulk(objs []interface{}, options ...Option) error

	// Count counts the number of objects in the patchain that matches a query
	Count(q Query, out *int64, options ...Option) error

	// GetLast gets the last and most recent object that match the query
	GetLast(q Query, out interface{}, options ...Option) error

	// GetAll returns all the objects that match the query
	GetAll(q Query, out interface{}, options ...Option) error

	// GetValidObjectFields returns a slice of field names or column
	// names that can be included in a JSQ query.
	GetValidObjectFields() []string

	// NewQuery returns a JSQ query parser. See http://github.com/ncodes/jsq
	NewQuery() jsq.Query

	// Begin returns a DB object with an active transaction
	Begin() DB

	// Transact starts a transaction and calls the TxFunc. It will auto commit or rollback
	// the transaction if TxFunc returns nil or error respectively.
	Transact(TxFunc) error

	// TransactWithDB is like Transact but it allows the use of external transaction db/session
	// and will only perform auto commit/rollback if finishTx is set to true.
	TransactWithDB(db DB, finishTx bool, txF TxFunc) error

	// Commit commits the current transaction
	Commit() error

	// Rollback rolls back the current transaction
	Rollback() error

	// NoLogging disables logging
	NoLogging()

	// Close frees up all resources held by the object
	Close() error
}

// QueryOption provides fields that can be used to
// alter a query
type QueryOption struct {
	OrderBy string
	Limit   int64
	Offset  int64
}

// Option represents an option to be used in a DB operation
type Option interface {
	GetName() string
	GetValue() interface{}
}

// Query represents all kinds of database queries
type Query interface {
	GetQueryParams() *QueryParams
}

// Expr describes a query expression
type Expr struct {
	Expr string
	Args []interface{}
}

// QueryParams represents object query options
type QueryParams struct {
	Expr          Expr   `json:"-" structs:"-" mapstructure:"-" gorm:"-"`
	KeyStartsWith string `json:"-" structs:"-" mapstructure:"-" gorm:"-"`
	OrderBy       string `json:"-" structs:"-" mapstructure:"-" gorm:"-"`
	Limit         int    `json:"-" structs:"-" mapstructure:"-" gorm:"-"`
}

// TxFunc is called after a transaction is started.
// DB represents the transaction object
type TxFunc func(DB, CommitFunc, RollbackFunc) error

// RollbackFunc represents a function that rolls back a transaction
type RollbackFunc func() error

// CommitFunc represents a function that commits a transaction
type CommitFunc func() error
