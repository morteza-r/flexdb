package flexdb

import (
	"errors"
	"github.com/mdaliyan/bucket"
	"sync"
)

var counter int

//TODO dont index text with long len

type Database struct {
	Tables sync.Map `json:"tables"` //map[string]Table
	Bucket bucket.Bucket
}

func NewDb() *Database {
	db := Database{
		Bucket: bucket.New(1, func(items []interface{}) {}),
	}
	db.Bucket.SetCallback(db.BucketFunc)

	return &db
}

func (db *Database) Run(q *Query) (result interface{}, err error) {
	err = q.Check()
	if err != nil {
		return
	}
	switch q.Type {
	case "get":
		result, err = db.GetQuery(*q)
		if err != nil {
			return
		}
	case "exists":
		result, err = db.ExistsQuery(*q)
		if err != nil {
			return
		}
	case "add":
		result, err = db.AddQuery(*q)
		if err != nil {
			return
		}
	case "replace":
		result, err = db.ReplaceQuery(*q)
		if err != nil {
			return
		}
	case "update":
		result, err = db.UpdateQuery(*q)
		if err != nil {
			return
		}
	case "delete":
		result, err = db.DeleteQuery(*q)
		if err != nil {
			return
		}
	default:
		err = errors.New("query type is unknown")
	}

	return
}
