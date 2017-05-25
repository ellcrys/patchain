package tables

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestObject(t *testing.T) {
	Convey("Object", t, func() {
		Convey(".Init", func() {
			Convey("Should initialize an object just once", func() {
				obj := Object{
					CreatorID:   "creator_1",
					OwnerID:     "owner_1",
					PartitionID: "partition_1",
				}
				obj.Init()
				So(obj.ID, ShouldNotBeEmpty)
				So(obj.Timestamp, ShouldNotBeNil)

				id := obj.ID
				ts := obj.Timestamp
				obj.Init()
				So(obj.ID, ShouldEqual, id)
				So(obj.Timestamp, ShouldEqual, ts)
			})
		})

		Convey(".ComputeHash", func() {
			Convey("Should create hash and must return same hash as long as object remains unchanged", func() {
				obj := Object{
					CreatorID:   "creator_1",
					OwnerID:     "owner_1",
					PartitionID: "partition_1",
				}
				obj.Init()
				So(obj.Hash, ShouldBeEmpty)
				obj.ComputeHash()
				So(obj.Hash, ShouldNotBeEmpty)

				hash := obj.Hash
				obj.ComputeHash()
				So(obj.Hash, ShouldEqual, hash)
			})
		})
	})
}
