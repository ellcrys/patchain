package tables

import (
	"fmt"
	"time"

	"github.com/ellcrys/util"
	"github.com/ncodes/patchain"
)

// SchemaVersion describes the version the object's schema/field make up.
var SchemaVersion = "1"

// Object represents a transaction created by an identity
type Object struct {
	ID            string               `json:"id,omitempty" structs:"id,omitempty" mapstructure:"id,omitempty" gorm:"type:varchar(36);primary_key"`
	OwnerID       string               `json:"owner_id,omitempty" structs:"owner_id,omitempty" mapstructure:"owner_id,omitempty" gorm:"type:varchar(36)"`
	CreatorID     string               `json:"creator_id,omitempty" structs:"creator_id,omitempty" mapstructure:"creator_id,omitempty" gorm:"type:varchar(36);index:idx_creator_id"`
	PartitionID   string               `json:"partition_id,omitempty" structs:"partition_id,omitempty" mapstructure:"partition_id,omitempty" gorm:"type:varchar(36);index:idx_prtn_id"`
	Key           string               `json:"key,omitempty" structs:"key,omitempty" mapstructure:"key,omitempty" gorm:"type:varchar(64);index:idx_key"`
	Value         string               `json:"value,omitempty" structs:"value,omitempty" mapstructure:"value,omitempty" gorm:"type:varchar(64000);index:idx_value"`
	Protected     bool                 `json:"protected,omitempty" structs:"protected,omitempty" mapstructure:"protected,omitempty" gorm:"index:idx_protected"`
	RefOnly       bool                 `json:"ref_only,omitempty" structs:"ref_only,omitempty" mapstructure:"ref_only,omitempty" gorm:"index:idx_ref_only"`
	Timestamp     int64                `json:"timestamp,omitempty" structs:"timestamp,omitempty" mapstructure:"timestamp,omitempty" gorm:"index:idx_timestamp"`
	PrevHash      string               `json:"prev_hash,omitempty" structs:"prev_hash,omitempty" mapstructure:"prev_hash,omitempty" gorm:"type:varchar(64);index:idx_prev_hash"`
	PeerHash      string               `json:"peer_hash,omitempty" structs:"peer_hash,omitempty" mapstructure:"peer_hash,omitempty" gorm:"type:varchar(64);index:idx_peer_hash"`
	Hash          string               `json:"hash,omitempty" structs:"hash,omitempty" mapstructure:"hash,omitempty" gorm:"type:varchar(64);index:idx_hash"`
	SchemaVersion string               `json:"schema_version,omitempty" structs:"schema_version,omitempty" mapstructure:"schema_version,omitempty" gorm:"type:varchar(64);index:idx_sch_ver"`
	Ref1          string               `json:"ref1,omitempty" structs:"ref1,omitempty" mapstructure:"ref1,omitempty" gorm:"type:varchar(64);index:idx_ref1"`
	Ref2          string               `json:"ref2,omitempty" structs:"ref2,omitempty" mapstructure:"ref2,omitempty" gorm:"type:varchar(64);index:idx_ref2"`
	Ref3          string               `json:"ref3,omitempty" structs:"ref3,omitempty" mapstructure:"ref3,omitempty" gorm:"type:varchar(64);index:idx_ref3"`
	Ref4          string               `json:"ref4,omitempty" structs:"ref4,omitempty" mapstructure:"ref4,omitempty" gorm:"type:varchar(64);index:idx_ref4"`
	Ref5          string               `json:"ref5,omitempty" structs:"ref5,omitempty" mapstructure:"ref5,omitempty" gorm:"type:varchar(64);index:idx_ref5"`
	Ref6          string               `json:"ref6,omitempty" structs:"ref6,omitempty" mapstructure:"ref6,omitempty" gorm:"type:varchar(64);index:idx_ref6"`
	Ref7          string               `json:"ref7,omitempty" structs:"ref7,omitempty" mapstructure:"ref7,omitempty" gorm:"type:varchar(64);index:idx_ref7"`
	Ref8          string               `json:"ref8,omitempty" structs:"ref8,omitempty" mapstructure:"ref8,omitempty" gorm:"type:varchar(64);index:idx_ref8"`
	Ref9          string               `json:"ref9,omitempty" structs:"ref9,omitempty" mapstructure:"ref9,omitempty" gorm:"type:varchar(64);index:idx_ref9"`
	Ref10         string               `json:"ref10,omitempty" structs:"ref10,omitempty" mapstructure:"ref10,omitempty" gorm:"type:varchar(64);index:idx_ref10"`
	QueryParams   patchain.QueryParams `json:"-" structs:"-" mapstructure:"-" gorm:"-"`
}

// Init sets defaults values for specific fields
// if they haven't been set.
func (o *Object) Init() *Object {

	if o.ID == "" {
		o.ID = util.UUID4()
	}

	// set the previous hash to the sha256 hash off the object's ID it not already set.
	if o.PrevHash == "" {
		o.PrevHash = util.Sha256(o.ID)
	}

	if o.Timestamp == 0 {
		o.Timestamp = time.Now().UnixNano()
	}

	if o.SchemaVersion == "" {
		o.SchemaVersion = SchemaVersion
	}

	return o
}

// ComputeHash computes a SHA256 has of the object
func (o *Object) ComputeHash() *Object {

	if o.SchemaVersion == "1" {
		o.Hash = util.Sha256(fmt.Sprintf("%s/%s/%s/%s/%s/%s/%v/%v/%d/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s/%s",
			o.ID, o.OwnerID, o.CreatorID, o.PartitionID,
			util.Sha256(o.Key), util.Sha256(o.Value),
			o.Protected,
			o.RefOnly,
			o.Timestamp,
			o.PrevHash,
			o.SchemaVersion,
			o.Ref1, o.Ref2, o.Ref3, o.Ref4, o.Ref5, o.Ref6, o.Ref7, o.Ref8, o.Ref9, o.Ref10,
		))
	}

	return o
}

// ComputePeerHash computes the peer hash. A peer hash binds the current object
// to the object next object.
func (o *Object) ComputePeerHash(nextObjHash string) *Object {
	if o.SchemaVersion == "1" {
		o.PeerHash = util.Sha256(fmt.Sprintf("%s/%s", o.Hash, nextObjHash))
	}
	return o
}

// GetQueryParams returns the query parameters attached to the object
func (o *Object) GetQueryParams() *patchain.QueryParams {
	return &o.QueryParams
}
