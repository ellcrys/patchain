package object

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/ellcrys/util"
	"github.com/jinzhu/gorm"

	"github.com/ellcrys/patchain"
	"github.com/ellcrys/patchain/cockroach"
	"github.com/ellcrys/patchain/cockroach/tables"
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

func TestObject(t *testing.T) {

	if err := createDb(t); err != nil {
		t.Fatalf("failed to create test database. %s", err)
	}
	defer dropDB(t)

	cdb := cockroach.NewDB()
	cdb.ConnectionString = conStrWithDB
	cdb.NoLogging()
	if err := cdb.Connect(10, 5); err != nil {
		t.Fatalf("failed to connect to database. %s", err)
	}

	if err := cdb.CreateTables(); err != nil {
		t.Fatalf("failed to create tables. %s", err)
	}

	obj := NewObject(cdb)

	Convey("Object", t, func() {
		Convey(".Create", func() {
			Convey("Should initialize and create an object", func() {
				o := &tables.Object{Key: "some_key", Value: "some_value", PrevHash: util.UUID4()}
				So(o.ID, ShouldBeEmpty)
				So(o.Timestamp, ShouldEqual, 0)
				So(o.Hash, ShouldBeEmpty)
				err := obj.Create(o)
				So(err, ShouldBeNil)
				So(o.ID, ShouldNotBeEmpty)
				So(o.Timestamp, ShouldNotBeEmpty)
				So(o.Hash, ShouldNotBeEmpty)

				Convey("Should create duplicate key object with same key", func() {
					o.ID = util.UUID4()
					o.PrevHash = util.UUID4()
					err := obj.Create(o)
					So(err, ShouldBeNil)
					count := int64(0)
					err = obj.db.Count(&tables.Object{}, &count)
					So(err, ShouldBeNil)
					So(count, ShouldEqual, 2)
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".CreateOnce", func() {
				Convey("Should initialize and create an object", func() {
					o := &tables.Object{Key: "some_key", Value: "some_value", PrevHash: util.UUID4()}
					So(o.ID, ShouldBeEmpty)
					So(o.Timestamp, ShouldEqual, 0)
					So(o.Hash, ShouldBeEmpty)
					err := obj.CreateOnce(o)
					So(err, ShouldBeNil)
					So(o.ID, ShouldNotBeEmpty)
					So(o.Timestamp, ShouldNotBeEmpty)
					So(o.Hash, ShouldNotBeEmpty)

					Convey("Should not create duplicate key object and also return no error", func() {
						o.ID = util.UUID4()
						o.PrevHash = util.UUID4()
						err := obj.CreateOnce(o)
						So(err, ShouldBeNil)
						count := int64(0)
						err = obj.db.Count(&tables.Object{Key: o.Key}, &count)
						So(err, ShouldBeNil)
						So(count, ShouldEqual, 1)
					})
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".RequiresRetry", func() {
				err := fmt.Errorf(`pq: duplicate key value (prev_hash)=('stuff') violates unique constraint "idx_name_prev_hash"`)
				So(obj.RequiresRetry(err), ShouldEqual, true)
				err = fmt.Errorf(`pq: some text retry transaction`)
				So(obj.RequiresRetry(err), ShouldEqual, true)
				err = fmt.Errorf(`pq: some text restart transaction`)
				So(obj.RequiresRetry(err), ShouldEqual, true)
			})

			Convey(".CreatePartitions", func() {

				Convey("Should successfully create initial partitions", func() {
					partitions, err := obj.CreatePartitions(3, "owner_id", "creator_id")
					So(err, ShouldBeNil)
					So(len(partitions), ShouldEqual, 3)

					Convey("All new partitions must include genesis pair objects", func() {
						for _, partition := range partitions {
							var all []tables.Object
							err := cdb.GetAll(&tables.Object{PartitionID: partition.ID, QueryParams: patchain.QueryParams{OrderBy: "timestamp asc"}}, &all)
							So(err, ShouldBeNil)
							So(len(all), ShouldEqual, 2)
							So(all[0].Key, ShouldEqual, "$genesis/1")
							So(all[1].Key, ShouldEqual, "$genesis/2")
						}
					})

					Convey("first partition prev hash must be equal to the SHA256 hash of the ID", func() {
						So(partitions[0].PrevHash, ShouldEqual, util.Sha256(partitions[0].ID))
					})

					Convey("partitions must be chained to the partition before it", func() {
						So(partitions[1].PrevHash, ShouldEqual, partitions[0].Hash)
						So(partitions[2].PrevHash, ShouldEqual, partitions[1].Hash)
					})

					Convey("New partition must reference the prev hash of the last included partition", func() {
						latestPartitions, err := obj.CreatePartitions(2, "owner_id", "creator_id")
						So(err, ShouldBeNil)
						So(len(latestPartitions), ShouldEqual, 2)

						Convey("first partition must reference the prev hash of the last partition", func() {
							So(latestPartitions[0].PrevHash, ShouldEqual, partitions[2].Hash)
							So(latestPartitions[1].PrevHash, ShouldEqual, latestPartitions[0].Hash)
						})
					})
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".MustCreatePartitions", func() {

				Convey("Should successfully create initial partitions", func() {
					partitions, err := obj.MustCreatePartitions(3, "owner_id", "creator_id")
					So(err, ShouldBeNil)
					So(len(partitions), ShouldEqual, 3)

					Convey("first partition prev hash must be equal to the SHA256 hash of the ID", func() {
						So(partitions[0].PrevHash, ShouldEqual, util.Sha256(partitions[0].ID))
					})

					Convey("partitions must be chained to the partition before it", func() {
						So(partitions[1].PrevHash, ShouldEqual, partitions[0].Hash)
						So(partitions[2].PrevHash, ShouldEqual, partitions[1].Hash)
					})

					Convey("New partition must reference the prev hash of the last included partition", func() {
						latestPartitions, err := obj.MustCreatePartitions(2, "owner_id", "creator_id")
						So(err, ShouldBeNil)
						So(len(latestPartitions), ShouldEqual, 2)

						Convey("first partition must reference the prev hash of the last partition", func() {
							So(latestPartitions[0].PrevHash, ShouldEqual, partitions[2].Hash)
							So(latestPartitions[1].PrevHash, ShouldEqual, latestPartitions[0].Hash)
						})
					})
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".GetLast", func() {
				Convey("Should return ErrNotFound if no object was found", func() {
					_, err := obj.GetLast(&tables.Object{Key: "some_key"})
					So(err, ShouldNotBeNil)
					So(err, ShouldEqual, patchain.ErrNotFound)
				})

				Convey("Should return last added object matching the query", func() {
					o := &tables.Object{Key: "some_key", Value: "some_value", PrevHash: util.UUID4()}
					o2 := &tables.Object{Key: "some_key", Value: "some_value_2", PrevHash: util.UUID4()}
					o3 := &tables.Object{Key: "some_key", Value: "some_value_3", PrevHash: util.UUID4()}
					err := obj.Create(o)
					So(err, ShouldBeNil)
					err = obj.Create(o2)
					So(err, ShouldBeNil)
					err = obj.Create(o3)
					So(err, ShouldBeNil)

					last, err := obj.GetLast(&tables.Object{Key: "some_key"})
					So(err, ShouldBeNil)
					So(last, ShouldResemble, o3)
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".All", func() {
				Convey("Should return ErrNotFound if no object was found", func() {
					objs, err := obj.All(&tables.Object{Key: "some_key"})
					So(err, ShouldBeNil)
					So(objs, ShouldBeEmpty)
				})

				Convey("Should successfully fetch all objects", func() {
					o := &tables.Object{Key: "some_key", Value: "some_value", PrevHash: util.UUID4()}
					o2 := &tables.Object{Key: "some_key", Value: "some_value_2", PrevHash: util.UUID4()}
					err := obj.Create(o)
					So(err, ShouldBeNil)
					err = obj.Create(o2)

					objs, err := obj.All(&tables.Object{Key: "some_key"})
					So(err, ShouldBeNil)
					So(len(objs), ShouldEqual, 2)
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".selectPartition", func() {
				Convey("Should return nil if no partition is passed", func() {
					selected := obj.selectPartition(nil)
					So(selected, ShouldBeNil)
				})

				Convey("Should return the only partition if only one partition is passed", func() {
					partitions := []*tables.Object{{ID: "some_id"}}
					selected := obj.selectPartition(partitions)
					So(selected, ShouldNotBeNil)
					So(selected, ShouldResemble, partitions[0])
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})

			Convey(".Put", func() {
				Convey("Should return error if value passed as object has invalid type", func() {
					err := obj.Put("a_string")
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "unsupported object type")
				})

				Convey("Should return error if no object is passed", func() {
					err := obj.Put([]*tables.Object{})
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "no object to put")
				})

				Convey("Should return error if an object does not have an owner id", func() {
					objs := []*tables.Object{
						{Key: "key_1", OwnerID: "some_owner_id"},
						{Key: "key_2", CreatorID: "some_creator_id"},
					}
					err := obj.Put(objs)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "object 1: object does not have an owner")
				})

				Convey("Should return error if an object has a different owner than others", func() {
					objs := []*tables.Object{
						{Key: "key_1", OwnerID: "some_owner_id"},
						{Key: "key_2", OwnerID: "some_other_owner_id"},
					}
					err := obj.Put(objs)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "object 1: has a different owner")
				})

				Convey("Should return error if owner has no partition", func() {
					objs := []*tables.Object{
						{Key: "key_1", OwnerID: "some_owner_id"},
						{Key: "key_2", OwnerID: "some_owner_id"},
					}
					err := obj.Put(objs)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "failed to put object(s): owner has no partition")
				})

				Convey("Should successfully add initial chained objects", func() {
					ownerID := util.RandString(10)
					identity := MakeIdentityObject(ownerID, ownerID, "email@email.com", "some_pass", true)
					err := obj.Create(identity)
					So(err, ShouldBeNil)

					partitions, err := obj.CreatePartitions(1, ownerID, ownerID)
					So(err, ShouldBeNil)

					objs := []*tables.Object{
						{Key: "key_1", OwnerID: ownerID, SchemaVersion: "1"},
						{Key: "key_2", OwnerID: ownerID, SchemaVersion: "1"},
						{Key: "key_3", OwnerID: ownerID, SchemaVersion: "1"},
					}
					err = obj.Put(objs)
					So(err, ShouldBeNil)

					Convey("Should return error if partition has no genesis object", func() {

						ownerID := util.RandString(10)
						identity := MakeIdentityObject(ownerID, ownerID, "email@email.com", "some_pass", true)
						err := obj.Create(identity)
						So(err, ShouldBeNil)

						err = cdb.Create(MakePartitionObject("a_partition", ownerID, ownerID))
						So(err, ShouldBeNil)
						err = obj.Put(&tables.Object{Key: "key_1", OwnerID: ownerID, SchemaVersion: "1"})
						So(err, ShouldNotBeNil)
						So(err.Error(), ShouldContainSubstring, "no genesis object in the partition")
					})

					Convey("third object of the partition must reference the hash of $genesis/2 as its PrevHash", func() {
						var genesisPair []tables.Object
						err := cdb.GetAll(&tables.Object{
							PartitionID: partitions[0].ID,
							QueryParams: patchain.QueryParams{
								OrderBy: "timestamp asc",
							},
						}, &genesisPair)
						So(err, ShouldBeNil)
						So(genesisPair[1].Key, ShouldEqual, "$genesis/2")
						So(genesisPair[1].Hash, ShouldEqual, objs[0].PrevHash)
						So(genesisPair[1].PeerHash, ShouldResemble, genesisPair[1].ComputePeerHash(objs[0].Hash).PeerHash)
					})

					Convey("all objects must be chained", func() {
						So(objs[0].Hash, ShouldEqual, objs[1].PrevHash)
						So(objs[1].Hash, ShouldEqual, objs[2].PrevHash)
					})

					Convey("all objects with an object after it must have a valid peer hash", func() {
						So(objs[0].PeerHash, ShouldResemble, objs[0].ComputePeerHash(objs[1].Hash).PeerHash)
						So(objs[1].PeerHash, ShouldResemble, objs[1].ComputePeerHash(objs[2].Hash).PeerHash)

						Convey("an object with no peer must have no peer hash", func() {
							So(objs[2].PeerHash, ShouldBeEmpty)
						})
					})

					Convey("Should successfully add an additional object to the non-empty partition", func() {
						o := &tables.Object{Key: "key_1", OwnerID: ownerID}
						err := obj.Put(o)
						So(err, ShouldBeNil)

						Convey("new object must have the hash of the previous object as the value of its prev hash", func() {
							So(objs[2].Hash, ShouldEqual, o.PrevHash)
						})

						Convey("preceding object must have a peer hash", func() {
							var newObj2 tables.Object
							err := cdb.GetLast(&tables.Object{ID: objs[2].ID}, &newObj2)
							So(err, ShouldBeNil)
							So(newObj2.PeerHash, ShouldNotBeNil)
							So(newObj2.PeerHash, ShouldResemble, newObj2.ComputePeerHash(o.Hash).PeerHash)
						})
					})
				})

				Reset(func() {
					clearTable(cdb.GetConn().(*gorm.DB), "objects")
				})
			})
		})
	})
}
