package flexdb

import (
	"reflect"
	"strings"
)

func Convert(item interface{}) (doc *Doc) {
	doc = NewDoc()
	fieldsMap := toMap(item)
	for key, value := range fieldsMap {
		doc.Fields[key] = value
		//doc.Fields.Store(key, value)
	}

	return
}

func toMap(input interface{}) (output map[string]interface{}) {
	output = make(map[string]interface{})
	inputV := reflect.ValueOf(input)
	inputT := inputV.Type()
	for i := 0; i < inputV.NumField(); i++ {
		if inputV.Field(i).CanInterface() {
			key := strings.ToLower(inputT.Field(i).Name)
			if inputV.Field(i).Kind() == reflect.Struct {
				output[key] = toMap(inputV.Field(i).Interface())
			} else {
				output[key] = inputV.Field(i).Interface()
			}
		}
	}

	return
}
