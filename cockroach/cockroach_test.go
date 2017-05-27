package cockroach

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/ellcrys/util"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/ncodes/patchain"
	"github.com/ncodes/patchain/cockroach/tables"
	. "github.com/smartystreets/goconvey/convey"
)

var testDB *sql.DB

var dbName = "test_" + strings.ToLower(util.RandString(5))
var conStr = "postgresql://root@localhost:26257?sslmode=disable"
var conStrWithDB = "postgresql://root@localhost:26257/" + dbName + "?sslmode=disable"

func init() {
	var err error
	testDB, err = sql.Open("postgres", conStr)
	if err != nil {
		panic(fmt.Errorf("failed to connect to database: %s", err))
	}
}

func createDb(t *testing.T) error {
	_, err := testDB.Query(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	return err
}

func dropDB(t *testing.T) error {
	_, err := testDB.Query(fmt.Sprintf("DROP DATABASE %s;", dbName))
	return err
}

func clearTable(db *gorm.DB, tables ...string) error {
	_, err := db.CommonDB().Exec("TRUNCATE " + strings.Join(tables, ","))
	if err != nil {
		return err
	}
	return nil
}

type SampleTbl struct {
	Col string
}

func TestCockroach(t *testing.T) {

	if err := createDb(t); err != nil {
		t.Fatalf("failed to create test database. %s", err)
	}
	defer dropDB(t)

	cdb := NewDB()
	cdb.ConnectionString = conStrWithDB
	cdb.NoLogging()

	Convey("Cockroach", t, func() {

		Convey(".Connect", func() {
			Convey("Should successfully connect to database", func() {
				err := cdb.Connect(10, 5)
				So(err, ShouldBeNil)

				Convey(".GetConn", func() {
					Convey("Should successfully return the underlying db connection", func() {
						db := cdb.GetConn()
						So(db, ShouldNotBeNil)
						So(db, ShouldResemble, cdb.db)
					})
				})

				Convey(".SetConn", func() {
					Convey("Should successfully set connection", func() {
						existingConn := cdb.GetConn()
						newConn, err := gorm.Open("postgres", conStrWithDB)
						So(err, ShouldBeNil)
						So(existingConn.(*gorm.DB), ShouldNotResemble, newConn)
						cdb.SetConn(newConn)
						existingConn = cdb.GetConn()
						So(existingConn.(*gorm.DB), ShouldResemble, newConn)
					})
				})

				Convey(".hasTable", func() {
					Convey("Should return false if table does not exist", func() {
						So(cdb.hasTable(&SampleTbl{}), ShouldEqual, false)
					})
					Convey("Should true if table exists", func() {
						type SampleTbl2 struct {
							Name string
						}
						db, _ := gorm.Open("postgres", conStrWithDB)
						err := db.CreateTable(&SampleTbl2{}).Error
						So(err, ShouldBeNil)
						So(cdb.hasTable(&SampleTbl2{}), ShouldEqual, true)
						db.DropTable(&SampleTbl2{})
						db.Close()
					})
				})

				Convey(".createTableIfNotExist", func() {
					Convey("Should successfully create table if not existing", func() {
						tblName := "sample_tbls"
						db, _ := gorm.Open("postgres", conStrWithDB)
						So(db.HasTable(tblName), ShouldEqual, false)
						err := cdb.createTableIfNotExist(&SampleTbl{})
						So(err, ShouldBeNil)
						So(db.HasTable(tblName), ShouldEqual, true)
						db.DropTable(&SampleTbl{})
						db.Close()
					})
				})

				Convey(".CreateTables", func() {
					Convey("Should successfully create all tables", func() {
						err := cdb.CreateTables()
						So(err, ShouldBeNil)
						db, _ := gorm.Open("postgres", conStrWithDB)
						So(db.HasTable("objects"), ShouldEqual, true)
						db.Close()
					})
				})

				Convey(".getValidObjectFields", func() {
					Convey("Should not include blacklisted fields", func() {
						fields := cdb.getValidObjectFields()
						So(fields, ShouldNotContain, blacklistedFields)
					})
				})

				Convey(".getDBTxFromOption", func() {
					Convey("Should successfully return database object included in the options", func() {
						_db := &DB{ConnectionString: conStrWithDB}
						opts := []patchain.Option{&patchain.UseDBOption{DB: _db, Finish: true}}
						_db2, finishTx := cdb.getDBTxFromOption(opts, nil)
						So(_db, ShouldResemble, _db2)
						So(finishTx, ShouldEqual, true)
					})

					Convey("Should successfully return the fallback database object when no db option is included in the options", func() {
						_db := &DB{ConnectionString: conStrWithDB}
						opts := []patchain.Option{}
						_db2, finishTx := cdb.getDBTxFromOption(opts, nil)
						So(nil, ShouldResemble, _db2)
						So(finishTx, ShouldEqual, false)
						_db2, finishTx = cdb.getDBTxFromOption(opts, _db)
						So(_db, ShouldResemble, _db2)
						So(finishTx, ShouldEqual, false)
					})
				})

				Convey(".Create", func() {

					Convey("Should successfully create object", func() {
						o := tables.Object{ID: util.UUID4()}
						err := cdb.Create(&o)
						So(err, ShouldBeNil)
						var actual tables.Object
						cdb.db.Where(&o).First(&actual)
						So(o, ShouldResemble, actual)
					})

					Convey("Should be able to use externally created connection", func() {
						cdb.CreateTables()
						dbTx := cdb.Begin()

						o := tables.Object{ID: util.UUID4()}
						err := cdb.Create(&o, &patchain.UseDBOption{DB: dbTx})
						So(err, ShouldBeNil)
						dbTx.Rollback()

						count := 0
						cdb.db.Model(&o).Where(&o).Count(&count)
						So(count, ShouldEqual, 0)

						dbTx = cdb.Begin()
						err = cdb.Create(&o, &patchain.UseDBOption{DB: dbTx})
						So(err, ShouldBeNil)
						dbTx.Commit()

						cdb.db.Model(&o).Where(&o).Count(&count)
						So(count, ShouldEqual, 1)
					})
				})

				Convey("Should successfully create bulk objects", func() {
					objs := []*tables.Object{&tables.Object{ID: util.UUID4()}, &tables.Object{ID: util.UUID4()}}
					objs[0].Init()
					objs[1].Init()
					objsI, _ := util.ToSliceInterface(objs)
					err := cdb.CreateBulk(objsI)
					So(err, ShouldBeNil)
					var actual tables.Object
					var actual2 tables.Object
					err = cdb.db.Where(objs[0]).First(&actual).Error
					So(objs[0], ShouldResemble, &actual)
					So(err, ShouldBeNil)
					err = cdb.db.Where(objs[1]).First(&actual2).Error
					So(err, ShouldBeNil)
					So(objs[1], ShouldResemble, &actual2)
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".GetLast", func() {
				Convey("Should successfully return the last object matching the query", func() {
					key := util.RandString(5)
					obj1 := &tables.Object{Key: key}
					obj2 := &tables.Object{Key: key}
					objs := []*tables.Object{obj1.Init(), obj2.Init()}
					objsI, _ := util.ToSliceInterface(objs)
					_ = objsI
					err := cdb.CreateBulk(objsI)
					So(err, ShouldBeNil)
					var last tables.Object
					err = cdb.GetLast(&tables.Object{Key: key}, &last)
					So(err, ShouldBeNil)
					So(obj2, ShouldResemble, &last)
				})

				Convey("Should return ErrNoFound if nothing was found", func() {
					var last tables.Object
					err := cdb.GetLast(&tables.Object{Key: util.RandString(5)}, &last)
					So(err, ShouldEqual, patchain.ErrNotFound)
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".GetAll", func() {

				Convey("Should return ErrNoFound if nothing was found", func() {
					var all []tables.Object
					err := cdb.GetAll(&tables.Object{Key: util.RandString(5)}, &all)
					So(err, ShouldNotEqual, patchain.ErrNotFound)
					So(len(all), ShouldEqual, 0)
				})

				Convey("Should successfully return objects", func() {
					key := util.RandString(5)
					objs := []*tables.Object{&tables.Object{ID: util.UUID4(), Key: key}, &tables.Object{ID: util.UUID4(), Key: key}}
					objs[0].Init()
					objs[1].Init()
					objsI, _ := util.ToSliceInterface(objs)
					err := cdb.CreateBulk(objsI)

					So(err, ShouldBeNil)
					var all []tables.Object
					err = cdb.GetAll(&tables.Object{Key: key}, &all)
					So(err, ShouldNotEqual, patchain.ErrNotFound)
					So(len(all), ShouldEqual, 2)
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".Count", func() {
				Convey("Should successfully count objects that match a query", func() {
					key := util.RandString(5)
					objs := []*tables.Object{&tables.Object{ID: util.UUID4(), Key: key}, &tables.Object{ID: util.UUID4(), Key: key}}
					objs[0].Init()
					objs[1].Init()
					objsI, _ := util.ToSliceInterface(objs)
					err := cdb.CreateBulk(objsI)

					So(err, ShouldBeNil)
					var count int64
					err = cdb.Count(&tables.Object{Key: key}, &count)
					So(err, ShouldBeNil)
					So(count, ShouldEqual, 2)
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".getQueryModifiers - Tests query parameters", func() {
				Convey("KeyStartsWith", func() {
					Convey("Should return the last object with the matching start key", func() {
						key := "special_key_prefix/abc"
						obj := &tables.Object{ID: util.UUID4(), Key: key}
						err := cdb.Create(obj)
						So(err, ShouldBeNil)
						conn := cdb.GetConn().(*gorm.DB)
						modifiers := cdb.getQueryModifiers(&tables.Object{
							QueryParams: patchain.QueryParams{
								KeyStartsWith: "special_key_prefix",
							},
						})
						var last tables.Object
						err = conn.Scopes(modifiers...).Last(&last).Error
						So(err, ShouldBeNil)
						So(obj, ShouldResemble, &last)
					})

					Convey("Should return the objects ordered by a field in ascending and descending order", func() {
						objs := []*tables.Object{&tables.Object{ID: util.UUID4(), Key: "1"}, &tables.Object{ID: util.UUID4(), Key: "2"}}
						objs[0].Init()
						objs[1].Init()
						objsI, _ := util.ToSliceInterface(objs)
						err := cdb.CreateBulk(objsI)
						So(err, ShouldBeNil)
						conn := cdb.GetConn().(*gorm.DB)
						modifiers := cdb.getQueryModifiers(&tables.Object{
							QueryParams: patchain.QueryParams{
								OrderBy: "key desc",
							},
						})
						var res []*tables.Object
						err = conn.Scopes(modifiers...).Find(&res).Error
						So(err, ShouldBeNil)
						So(len(objs), ShouldEqual, 2)
						So(res[0], ShouldResemble, objs[1])
						So(res[1], ShouldResemble, objs[0])

						res = []*tables.Object{}
						modifiers = cdb.getQueryModifiers(&tables.Object{
							QueryParams: patchain.QueryParams{
								OrderBy: "key desc",
							},
						})
						err = conn.NewScope(nil).DB().Scopes(modifiers...).Find(&res).Error
						So(err, ShouldBeNil)
						So(len(objs), ShouldEqual, 2)
						So(res[1], ShouldResemble, objs[0])
						So(res[0], ShouldResemble, objs[1])
					})

					Convey("Should use QueryParam.Expr for query if set, instead of the query object", func() {
						key := util.RandString(5)
						obj := &tables.Object{ID: util.UUID4(), Key: key}
						err := cdb.Create(obj)
						So(err, ShouldBeNil)
						conn := cdb.GetConn().(*gorm.DB)
						res := []*tables.Object{}
						modifiers := cdb.getQueryModifiers(&tables.Object{
							Key: "some_key",
							QueryParams: patchain.QueryParams{
								Expr: patchain.Expr{
									Expr: "key = ?",
									Args: []interface{}{key},
								},
							},
						})
						err = conn.NewScope(nil).DB().Scopes(modifiers...).Find(&res).Error
						So(err, ShouldBeNil)
						So(len(res), ShouldEqual, 1)
						So(obj, ShouldResemble, res[0])
					})

					Convey("Should limit objects returned if Limit is set", func() {
						objs := []*tables.Object{&tables.Object{ID: util.UUID4(), Key: "1"}, &tables.Object{ID: util.UUID4(), Key: "2"}}
						objs[0].Init()
						objs[1].Init()
						objsI, _ := util.ToSliceInterface(objs)
						err := cdb.CreateBulk(objsI)
						So(err, ShouldBeNil)
						conn := cdb.GetConn().(*gorm.DB)
						modifiers := cdb.getQueryModifiers(&tables.Object{
							QueryParams: patchain.QueryParams{
								Limit: 1,
							},
						})
						var res []*tables.Object
						err = conn.Scopes(modifiers...).Find(&res).Error
						So(err, ShouldBeNil)
						So(len(res), ShouldEqual, 1)
						So(objs[1], ShouldResemble, res[0])
					})

					Reset(func() {
						clearTable(cdb.GetConn().(*gorm.DB), "objects")
					})
				})
			})
		})
	})
}
