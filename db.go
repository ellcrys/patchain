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
	Connect(maxOpenConn, maxIdleConn int) error
	GetConn() interface{}
	SetConn(interface{}) error
	CreateTables() error
	Create(obj interface{}, options ...Option) error
	CreateBulk(objs []interface{}, options ...Option) error
	Count(q Query, out *int64, options ...Option) error
	GetLast(q Query, out interface{}, options ...Option) error
	GetAll(q Query, out interface{}, options ...Option) error
	NewQuery() (jsq.Query, error)
	Begin() DB
	Transact(TxFunc) error
	TransactWithDB(db DB, finishTx bool, txF TxFunc) error
	Commit() error
	Rollback() error
	NoLogging()
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

// QueryParams represents object query options
type QueryParams struct {
	KeyStartsWith string `json:"-" structs:"-" mapstructure:"-" gorm:"-"`
	OrderBy       string `json:"-" structs:"-" mapstructure:"-" gorm:"-"`

	// If set to true, the query will be ordered by timestamp first before
	// including any value of OrderBy
	MustOrderByTimestampDesc bool `json:"-" structs:"-" mapstructure:"-" gorm:"-"`
}

// TxFunc is called after a transaction is started.
// DB represents the transaction object
type TxFunc func(DB, CommitFunc, RollbackFunc) error

// RollbackFunc represents a function that rolls back a transaction
type RollbackFunc func() error

// CommitFunc represents a function that commits a transaction
type CommitFunc func() error
