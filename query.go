package flexdb

import (
	"errors"
	"time"
)

type Query struct {
	Table     string  `json:"table"`
	QId       string  `json:"id"`
	Doc       *Doc    `json:"doc"`
	Where     []Where `json:"where"`
	WhereType string  `json:"where_type"`
	Order     Order   `json:"order"`
	Limit int           `json:"limit"`
	Type  string        `json:"type"`
	Took  time.Duration `json:"took"`
}

type Where struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

type Order struct {
	Field string `json:"field"`
	Type  string `json:"type"`
}

func (q *Query) Check() (err error) {
	err = q.CheckType()
	if err != nil {
		return
	}
	err = q.CheckTable()
	if err != nil {
		return
	}
	q.CheckWhereType()
	q.CheckOrder()

	return
}

func (q *Query) CheckTable() (err error) {
	if q.Table == "" {
		err = errors.New("table name is empty")
	}

	return
}

func (q *Query) CheckType() (err error) {
	if q.Type == "" {
		err = errors.New("query type is empty")
		return
	}

	return
}

func (q *Query) CheckWhereType() {
	if q.WhereType == "" {
		q.WhereType = "and"
	}
}

func (q *Query) CheckOrder() {
	if q.Order.Field == "" {
		q.Order.Field = "id"
		q.Order.Type = "float64"
	}
}
