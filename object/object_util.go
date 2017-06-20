package object

import (
	"fmt"

	"strings"

	"github.com/ellcrys/util"
	"github.com/ellcrys/patchain/cockroach/tables"
)

var (
	// PartitionPrefix is the prefix of a partition key
	PartitionPrefix = "$partition/"

	// IdentityPrefix is the prefix of an identity key
	IdentityPrefix = "$identity/"

	// MappingPrefix is the prefix of an object mappings
	MappingPrefix = "$mapping/"
)

// MakeIdentityKey creates an identity key
func MakeIdentityKey(email string) string {
	return fmt.Sprintf("%s%s", IdentityPrefix, email)
}

// MakePartitionKey creates a partition key
func MakePartitionKey(name string) string {
	return fmt.Sprintf("%s%s", PartitionPrefix, name)
}

// MakeMappingKey creates a mapping key
func MakeMappingKey(name string) string {
	return fmt.Sprintf("%s%s", MappingPrefix, name)
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

// SplitKey returns the prefix and name part of a system key
// such as partition, identity or mapping keys.
func SplitKey(key string) (string, string, error) {
	split := strings.SplitN(key, "/", 2)
	if split == nil || len(split) != 2 {
		return "", "", fmt.Errorf("invalid key format")
	}
	return split[0], split[1], nil
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

// MakeMappingObject creates a mapping object
func MakeMappingObject(ownerID, name, mappingJSON string) *tables.Object {
	po := tables.Object{
		OwnerID:   ownerID,
		CreatorID: ownerID,
		Key:       MakeMappingKey(name),
		Value:     mappingJSON,
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
// hash of the previous on their PrevHash field and each preceding object
// calculates its PeerHash which is the hash of its own hash and the hash of the object after
func MakeChain(objects ...*tables.Object) {
	for i, object := range objects {
		object.Init().ComputeHash()
		if i > 0 {
			prevObj := objects[i-1]
			object.PrevHash = prevObj.Hash
			object.ComputeHash()
			prevObj.ComputePeerHash(object.Hash)
		}
	}
}

// MakeGenesisPair creates two objects to be used as genesis object pairs
func MakeGenesisPair(ownerID, creatorID, partitionID, partitionHash string) []*tables.Object {
	pair := []*tables.Object{{
		OwnerID:       ownerID,
		CreatorID:     creatorID,
		PartitionID:   partitionID,
		Key:           "$genesis/1",
		SchemaVersion: "1",
		PrevHash:      partitionHash,
	}, {
		OwnerID:       ownerID,
		CreatorID:     creatorID,
		PartitionID:   partitionID,
		Key:           "$genesis/2",
		SchemaVersion: "1",
	}}
	MakeChain(pair...)
	return pair
}
