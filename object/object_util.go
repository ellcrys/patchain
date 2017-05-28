package object

import (
	"fmt"

	"github.com/ellcrys/util"
	"github.com/ncodes/patchain/cockroach/tables"
)

var (
	// PartitionPrefix is the prefix of a partition key
	PartitionPrefix = "partition/"

	// IdentityPrefix is the prefix of an identity key
	IdentityPrefix = "identity/"
)

// MakeIdentityKey creates an identity key
func MakeIdentityKey(email string) string {
	return fmt.Sprintf("%s%s", IdentityPrefix, email)
}

// MakePartitionKey creates a partition key
func MakePartitionKey(name string) string {
	return fmt.Sprintf("%s%s", PartitionPrefix, name)
}

// MakePartitionObject creates an object that describes a partition
func MakePartitionObject(name, ownerID, creatorID string) *tables.Object {
	po := tables.Object{
		OwnerID:   ownerID,
		CreatorID: creatorID,
		Key:       MakePartitionKey(name),
	}
	return po.Init()
}

// MakeIdentityObject creates an object that describes an identity
func MakeIdentityObject(ownerID, creatorID, email, password string, protected bool) *tables.Object {
	po := tables.Object{
		OwnerID:   ownerID,
		CreatorID: creatorID,
		Key:       MakeIdentityKey(email),
		Protected: protected,
	}
	return po.Init()
}

// MakeDeveloperIdentityObject creates an identity with developer related data like
// client id and client secret
func MakeDeveloperIdentityObject(ownerID, creatorID, email, password string, protected bool) *tables.Object {
	po := tables.Object{
		OwnerID:   ownerID,
		CreatorID: creatorID,
		Key:       MakeIdentityKey(email),
		Protected: protected,
		Ref1:      util.RandString(util.RandNum(26, 36)), // client id
		Ref2:      util.RandString(util.RandNum(26, 36)), // client secret
	}
	return po.Init()
}

// MakeChain takes objects and chains them together. Each object referencing the
// hash of the previous on their PrevHash field.
func MakeChain(objects ...*tables.Object) {
	for i, object := range objects {
		object.Init().ComputeHash()
		if i > 0 {
			prevObj := objects[i-1]
			object.PrevHash = prevObj.Hash
			object.ComputeHash()
		}
	}
}

// MakeChainWithPrefix takes objects and chains them together. Each object referencing the
// hash of the previous on their PrevHash field. prevHashPrefix is used as a flag to tell
// what kind of object is being chained or as a namespace for certain objects (e.g partitions).
func MakeChainWithPrefix(prevHashPrefix string, objects ...*tables.Object) {
	for i, object := range objects {
		object.Init().ComputeHash()
		if i > 0 {
			prevObj := objects[i-1]
			object.PrevHash = prevHashPrefix + "/" + prevObj.Hash
			object.ComputeHash()
		}
	}
}
