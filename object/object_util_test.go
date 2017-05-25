package object

import (
	"testing"

	"github.com/ellcrys/util"
	"github.com/ncodes/patchain/cockroach/tables"
	. "github.com/smartystreets/goconvey/convey"
)

func TestObjectUtil(t *testing.T) {
	Convey("ObjectUtil", t, func() {
		Convey(".MakeIdentityKey", func() {
			So(MakeIdentityKey("lana@gmail.com"), ShouldEqual, "identity/lana@gmail.com")
		})

		Convey(".MakePartitionKey", func() {
			So(MakePartitionKey("partition_a"), ShouldEqual, "partition/partition_a")
		})

		Convey(".MakePartitionObject", func() {
			obj := MakePartitionObject("partition_a", "owner_id", "creator_id")
			So(obj.CreatorID, ShouldEqual, "creator_id")
			So(obj.OwnerID, ShouldEqual, "owner_id")
			So(obj.Key, ShouldEqual, MakePartitionKey("partition_a"))
		})

		Convey(".MakeIdentityObject", func() {
			obj := MakeIdentityObject("owner_id", "creator_id", "lana@gmail.com", "some_password", true)
			So(obj.CreatorID, ShouldEqual, "creator_id")
			So(obj.OwnerID, ShouldEqual, "owner_id")
			So(obj.Key, ShouldEqual, MakeIdentityKey("lana@gmail.com"))
			So(obj.ID, ShouldNotBeEmpty)
			So(obj.Timestamp, ShouldNotBeEmpty)
		})

		Convey(".MakeChain", func() {
			Convey("Should successfully chain multiple objects", func() {
				objs := []*tables.Object{
					MakeIdentityObject("owner_id", "creator_id", "lana@gmail.com", "some_password", true),
					MakeIdentityObject("owner_id_2", "creator_id_2", "lana_2@gmail.com", "some_password_2", true),
					MakeIdentityObject("owner_id_3", "creator_id_3", "lana_3@gmail.com", "some_password_3", false),
				}
				MakeChain(objs...)
				So(objs[0].PrevHash, ShouldEqual, util.Sha256(objs[0].ID))
				So(objs[0].Hash, ShouldEqual, objs[1].PrevHash)
				So(objs[1].Hash, ShouldEqual, objs[2].PrevHash)
				MakeChain(objs...)
				So(objs[0].PrevHash, ShouldEqual, util.Sha256(objs[0].ID))
				So(objs[0].Hash, ShouldEqual, objs[1].PrevHash)
				So(objs[1].Hash, ShouldEqual, objs[2].PrevHash)
			})
		})
	})
}
