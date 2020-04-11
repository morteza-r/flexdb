package flexdb

import (
	"encoding/json"
	"errors"
)

type Doc struct {
	//Id     uint
	Fields map[string]interface{}
}

func NewDoc() *Doc {
	var doc Doc
	//Todo auto increment id
	doc.Fields = make(map[string]interface{})

	return &doc
}

func (d *Doc) GetId() (id float64, err error) {
	if d.IsEmpty() {
		return 0, errors.New("doc is empty")
	}
	if _, ok := d.Fields["id"]; !ok {
		return 0, errors.New("doc id is empty")
	}

	return d.Fields["id"].(float64), nil
}

func (d *Doc) SetId(id interface{}) (err error) {
	switch v := id.(type) {
	case float64:
		if v == 0 {
			return errors.New("id can not be empty")
		}
	default:
		return errors.New("id should be number")
	}
	//if d.IsEmpty() {
	//	d.Fields = make(map[string]interface{})
	//}
	if id == 0 {
		return errors.New("id should no be zero")
	}
	d.Fields["id"] = id.(float64)

	return
}

func (d *Doc) FixId() {
	id, err := d.GetId()
	if err == nil {
		_ = d.SetId(id)
	}
}

func (d *Doc) Set(key string, value interface{}) (err error) {
	if key == "id" {
		err := d.SetId(value)
		if err != nil {
			return err
		}
	}
	//if d.IsEmpty() {
	//	d.Fields = make(map[string]interface{})
	//}
	d.Fields[key] = value

	return
}

func (d *Doc) Get(key string) (value interface{}, err error) {
	if key == "" {
		return nil, errors.New("key should not be empty")
	}
	if d.IsEmpty() {
		return nil, errors.New("doc is empty")
	}
	if _, ok := d.Fields[key]; !ok {
		return nil, errors.New("cant find key")
	}
	value = d.Fields[key]

	return
}

func (d *Doc) FillFields(fields map[string]interface{}) {
	for key, val := range fields {
		_ = d.Set(key, val)
	}
}

//func (d *Doc) Fill(item interface{}) (err error) {
//	if d.IsEmpty() {
//		d.Fields = make(map[string]interface{})
//	}
//	doc := Convert(item)
//	d.Fields = doc.Fields
//
//	return
//}

func (d *Doc) IsEmpty() bool {
	return len(d.Fields) == 0
}

func (d *Doc) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Fields)
}
