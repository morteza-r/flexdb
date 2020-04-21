package flexdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

func (db *Database) LoadTable(tableName *string) (t *Table, err error) {
	table, ok := db.Tables.Load(*tableName)
	if !ok {
		err = errors.New("table does not exist: " + *tableName)
		return
	}
	t = table.(*Table)

	return
}

func (db *Database) All(q Query, docs *[]Doc) (err error) {
	if q.Limit == 0 {
		q.Limit = 30
	}

	t, err := db.LoadTable(&q.Table)
	if err != nil {
		return
	}

	indexKey := q.Order.Field + "_" + q.Order.Type
	tableIndex, ok := t.Indexes.Load(indexKey)
	if !ok {
		err = errors.New("index with field path and type [" + q.Order.Field + " " + q.Order.Type + "] not exist")
		return
	}

	ti := tableIndex.(*[]IndexItem)
	for _, item := range *ti {
		doc, _ := t.Docs.Load(item.Id)
		*docs = append(*docs, doc.(Doc))
		if len(*docs) == q.Limit {
			break
		}
	}

	//table, _ = db.Tables.LoadOrStore(*tableName, &Table{})
	//t = table.(*Table)
	//t.Indexes.Range(func(key, tableIndex interface{}) bool {
	//	fmt.Println(key, len(*tableIndex.(*[]IndexItem)), tableIndex)
	//	return true
	//})
	//fmt.Println()

	return
}

func (db *Database) Get(tableName *string, doc *Doc) (err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	// load doc
	id, err := doc.GetId()
	if err != nil {
		return err
	}
	docFound, ok := t.Docs.Load(id)
	if !ok {
		return errors.New("doc not found")
	}
	*doc = docFound.(Doc)

	return
}

func (db *Database) MultiGet(tableName *string, idList []float64, docs *[]Doc) (err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	// load id
	for _, id := range idList {
		doc, _ := t.Docs.Load(id)
		*docs = append(*docs, doc.(Doc))
	}

	return
}

func (db *Database) Add(tableName *string, doc *Doc) (err error) {
	table, _ := db.Tables.LoadOrStore(*tableName, &Table{})
	t := table.(*Table)
	id, err := doc.GetId()
	if err != nil {
		indexKey := "id_float64"
		tableIndex, ok := t.Indexes.Load(indexKey)
		if !ok {
			id = 1
		} else {
			ti := tableIndex.(*[]IndexItem)
			if len(*ti) == 0 {
				id = 1
			} else {
				lastItem := (*ti)[len(*ti)-1]
				id = lastItem.Id + 1
			}
		}
		err = doc.SetId(id)
		if err != nil {
			return
		}
	}
	_, ok := t.Docs.Load(id)
	if ok {
		err = errors.New("duplicate doc id found")
		return
	}
	t.Docs.Store(id, *doc)

	// add doc
	db.Tables.Store(*tableName, t)

	// add to index
	db.Bucket.Push(BucketItem{
		Table: *tableName,
		Doc:   *doc,
		Type:  1,
	})

	return
}

func (db *Database) Replace(tableName *string, doc *Doc) (err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	//replace new doc
	id, err := doc.GetId()
	if err != nil {
		return err
	}
	t.Docs.Store(id, *doc)
	db.Tables.Store(*tableName, t)

	//update index
	db.Bucket.Push(BucketItem{
		Table: *tableName,
		Doc:   *doc,
		Type:  0,
	})

	return
}

func (db *Database) Update(tableName *string, doc *Doc) (err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	//load old doc
	id, err := doc.GetId()
	if err != nil {
		return err
	}
	oldDoc, ok := t.Docs.Load(id)

	//replace new doc
	if !ok {
		return errors.New("doc not found to update")
	} else {
		result := setNotZero(oldDoc.(Doc).Fields, doc.Fields)
		//(*doc).Fields = result.(sync.Map)
		(*doc).Fields = result.(map[string]interface{})
		t.Docs.Store(id, *doc)
		db.Tables.Store(*tableName, t)
	}

	//update indexes
	db.Bucket.Push(BucketItem{
		Table: *tableName,
		Doc:   *doc,
		Type:  0,
	})

	return
}

func (db *Database) Delete(tableName *string, doc *Doc) (err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	//delete doc
	id, err := doc.GetId()

	//fmt.Println("Delete id", id)
	if err != nil {
		return err
	}
	t.Docs.Delete(id)
	db.Tables.Store(*tableName, t)

	//delete from index
	db.Bucket.Push(BucketItem{
		Table: *tableName,
		Doc:   *doc,
		Type:  -1,
	})

	return
}

func (db *Database) Where(tableName *string, wheres *[]Where, whereType *string) (res []float64, err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	var lists [][]float64
	for _, where := range *wheres {
		indexKey := where.Field + "_" + reflect.TypeOf(where.Value).String()
		tableIndex, ok := t.Indexes.Load(indexKey)
		if !ok {
			lists = append(lists, []float64{})
			continue
		}

		ti := tableIndex.(*[]IndexItem)
		//TODO all searches are case insensitive fix it!
		if reflect.TypeOf(where.Value).String() == "string" {
			where.Value = strings.ToLower(where.Value.(string))
		}
		firstIndex := firstOccurrence(*ti, 0, len(*ti)-1, where.Value, len(*ti))
		lastIndex := lastOccurrence(*ti, 0, len(*ti)-1, where.Value, len(*ti))
		if firstIndex != -1 {
			if lastIndex != -1 {
				var resultRow []float64
				for ; firstIndex <= lastIndex; firstIndex++ {
					resultRow = append(resultRow, (*ti)[firstIndex].Id)
				}
				lists = append(lists, resultRow)
			} else {
				lists = append(lists, []float64{(*ti)[firstIndex].Id})
			}
		}
	}

	if *whereType == "or" {
		return or(lists), nil
	}

	return and(lists), nil
}

func (db *Database) Order(tableName *string, list []float64, field *string, fieldType *string, limit *int) (sortedList []float64, err error) {
	listLength := len(list)
	if listLength <= 1 {
		return list, nil
	}

	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	indexKey := *field + "_" + *fieldType
	tableIndex, ok := t.Indexes.Load(indexKey)
	if !ok {
		err = errors.New("index with field path and type [" + *field + " " + *fieldType + "] not exist")
		return
	}

	ti := tableIndex.(*[]IndexItem)
	for _, item := range *ti {
		if isExist(list, item.Id) {
			sortedList = append(sortedList, item.Id)
			lenSorted := len(sortedList)
			if lenSorted == listLength || lenSorted == *limit {
				break
			}
		}
	}

	return
}

func (db *Database) BucketFunc(items []interface{}) {
	bItem := items[0].(BucketItem)
	switch bItem.Type {
	case 1:
		err := db.addToIndex(&bItem.Table, &bItem.Doc)
		if err != nil {
			fmt.Println(err)
		}
	case 0:
		err := db.removeFromIndex(&bItem.Table, &bItem.Doc)
		if err != nil {
			fmt.Println(err)
		}
		err = db.addToIndex(&bItem.Table, &bItem.Doc)
		if err != nil {
			fmt.Println(err)
		}
	case -1:
		err := db.removeFromIndex(&bItem.Table, &bItem.Doc)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (db *Database) addToIndex(tableName *string, doc *Doc) (err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	id, err := doc.GetId()
	if err != nil {
		return err
	}

	iMap := make(map[string]interface{})
	indexMap(doc.Fields, "", &iMap)

	for key, val := range iMap {
		valType := reflect.TypeOf(val).String()
		if val == nil || valType == "[]interface {}" {
			continue
		}
		indexItem := IndexItem{
			Id:    id,
			Value: val,
		}
		indexKey := key + "_" + reflect.TypeOf(val).String()
		tableIndex, _ := t.Indexes.LoadOrStore(indexKey, &([]IndexItem{}))
		ti := tableIndex.(*[]IndexItem)
		is := insertSorted(
			ti,
			&indexItem,
		)
		t.Indexes.Store(indexKey, &is)
	}
	db.Tables.Store(*tableName, t)

	return
}

func (db *Database) removeFromIndex(tableName *string, doc *Doc) (err error) {
	t, err := db.LoadTable(tableName)
	if err != nil {
		return
	}

	//load doc
	id, err := doc.GetId()
	if err != nil {
		return err
	}

	//remove old doc from all indexes
	t.Indexes.Range(func(key, tableIndex interface{}) bool {
		ti := tableIndex.(*[]IndexItem)
		for i := 0; i < len(*ti); i++ {
			if (*ti)[i].Id == id {
				t.Indexes.Store(key, removeFromIndexSlice(*ti, i))
				break
			}
		}
		return true
	})
	db.Tables.Store(*tableName, t)

	return
}

func or(lists [][]float64) (res []float64) {
	added := make(map[float64]bool)
	for _, ids := range lists {
		for _, id := range ids {
			_, ok := added[id]
			if ok {
				continue
			}
			res = append(res, id)
			added[id] = true
		}
	}

	return
}

func and(lists [][]float64) (res []float64) {
	if len(lists) == 1 {
		return lists[0]
	}
	tested := make(map[float64]bool)
	for ni, ids := range lists {
		for _, needId := range ids {
			_, ok := tested[needId]
			if ok {
				break
			}
			foundInAllRows := true
			for si, ids := range lists {
				if si == ni { //if same row continue
					continue
				}
				foundInRow := false
				for _, id := range ids {
					if needId == id {
						foundInRow = true
						break
					}
				}
				if !foundInRow {
					foundInAllRows = false
					break
				}
			}
			if foundInAllRows {
				res = append(res, needId)
			}
			tested[needId] = true
		}
	}

	return
}

func isExist(list []float64, item float64) bool {
	for _, current := range list {
		if current == item {
			return true
		}
	}

	return false
}

func indexMap(field interface{}, path string, iMap *map[string]interface{}) {
	if reflect.TypeOf(field).String() == "map[string]interface {}" {
		for key, val := range field.(map[string]interface{}) {
			var tempPath string
			if path == "" {
				tempPath = key
			} else {
				tempPath = strings.Join([]string{path, key}, ".")
			}
			if val == nil {
				continue
			}
			if reflect.TypeOf(val).String() == "map[string]interface {}" {
				indexMap(val, tempPath, iMap)
			} else {
				if reflect.TypeOf(val).String() == "string" {
					if val.(string) == "" {
						continue
					}
					val = strings.ToLower(val.(string))
				}
				(*iMap)[tempPath] = val
			}
		}
	}
}

func insertSorted(data *[]IndexItem, el *IndexItem) (res []IndexItem) {
	//if reflect.TypeOf(el.Value).String() == "string" {
	//	el.Value = strings.ToLower(el.Value.(string))
	//}
	if len(*data) == 0 {
		res = append(res, *el)
		return
	}
	count := 0
	index := sort.Search(len(*data), func(i int) bool {
		count++
		switch el.Value.(type) {
		case string:
			return (*data)[i].Value.(string) >= el.Value.(string)
		case float64:
			return (*data)[i].Value.(float64) >= el.Value.(float64)
		case bool:
			firstInt := 0
			secondInt := 0
			if (*data)[i].Value.(bool) {
				firstInt = 1
			}
			if el.Value.(bool) {
				secondInt = 1
			}
			return firstInt >= secondInt
		case nil:
			//fmt.Println("x is nil")
		default:
			//fmt.Println("type unknown")
		}
		return true
	})
	//fmt.Println("count", count)
	*data = append((*data)[:index], append([]IndexItem{*el}, (*data)[index:]...)...)
	res = *data

	return
}

func setNotZero(oldField interface{}, newField interface{}) (result interface{}) {
	var temp map[string]interface{}
	newType := reflect.TypeOf(newField).String()
	oldType := reflect.TypeOf(oldField).String()
	mapType := "map[string]interface {}"
	arrayType := "[]interface {}"

	if newType == oldType && newType == mapType {
		if oldType != mapType {
			result = reflect.ValueOf(newField).Interface()
			return
		}
		temp = oldField.(map[string]interface{})
		for key, value := range newField.(map[string]interface{}) {
			if key == "id" {
				continue
			}
			if value == nil {
				delete(temp, key)
				continue
			}
			if reflect.TypeOf(value).String() == "string" && value.(string) == "]rm[" {
				delete(temp, key)
				continue
			}
			if newType == arrayType && len(newField.([]interface{})) == 0 {
				delete(temp, key)
				continue
			}
			if _, ok := oldField.(map[string]interface{})[key]; !ok {
				temp[key] = value
			} else {
				temp[key] = setNotZero(oldField.(map[string]interface{})[key], value)
			}
		}
	} else {
		newValue := reflect.ValueOf(newField)
		if newValue.IsZero() {
			result = reflect.ValueOf(oldField).Interface()
		} else {
			result = newValue.Interface()
		}
		return
	}
	result = temp

	return
}

func removeFromIndexSlice(slice []IndexItem, index int) *[]IndexItem {
	if index == len(slice) {
		slice = slice[:index-1]
	} else {
		slice = append(slice[:index], slice[index+1:]...)
	}
	return &slice
}

func firstOccurrence(arr []IndexItem, low int, high int, needle interface{}, n int) int {
	counter++
	if high >= low {
		mid := low + (high-low)/2
		if (mid == 0 || compareInterface(needle, ">", arr[mid-1].Value)) &&
			compareInterface(arr[mid].Value, "==", needle) {
			return mid
		} else if compareInterface(needle, ">", arr[mid].Value) {
			return firstOccurrence(arr, mid+1, high, needle, n)
		} else {
			return firstOccurrence(arr, low, mid-1, needle, n)
		}
	}
	return -1
}

func lastOccurrence(arr []IndexItem, low int, high int, needle interface{}, n int) int {
	counter++
	if high >= low {
		mid := low + (high-low)/2
		if (mid == n-1 || compareInterface(needle, "<", arr[mid+1].Value)) &&
			compareInterface(arr[mid].Value, "==", needle) {
			return mid
		} else if compareInterface(needle, "<", arr[mid].Value) {
			return lastOccurrence(arr, low, mid-1, needle, n)
		} else {
			return lastOccurrence(arr, mid+1, high, needle, n)
		}
	}
	return -1
}

func compareInterface(first interface{}, operator string, second interface{}) bool {
	if reflect.TypeOf(first) != reflect.TypeOf(second) {
		return false
	}
	switch first.(type) {
	case string:
		switch operator {
		case "==":
			return first.(string) == second.(string)
		case "!=":
			return first.(string) != second.(string)
		case ">=":
			return first.(string) >= second.(string)
		case "<=":
			return first.(string) <= second.(string)
		case ">":
			return first.(string) > second.(string)
		case "<":
			return first.(string) < second.(string)
		}
	case float64:
		switch operator {
		case "==":
			return first.(float64) == second.(float64)
		case "!=":
			return first.(float64) != second.(float64)
		case ">=":
			return first.(float64) >= second.(float64)
		case "<=":
			return first.(float64) <= second.(float64)
		case ">":
			return first.(float64) > second.(float64)
		case "<":
			return first.(float64) < second.(float64)
		}
	case bool:
		firstInt := 0
		secondInt := 0
		if first.(bool) {
			firstInt = 1
		}
		if second.(bool) {
			secondInt = 1
		}
		switch operator {
		case "==":
			return firstInt == secondInt
		case "!=":
			return firstInt != secondInt
		case ">=":
			return firstInt >= secondInt
		case "<=":
			return firstInt <= secondInt
		case ">":
			return firstInt > secondInt
		case "<":
			return firstInt < secondInt
		}
	}

	return false
}

func getVal(input interface{}, path []string) (out interface{}, err error) {
	inputType := reflect.TypeOf(input).String()
	mapType := "map[string]interface {}"
	pathError := errors.New("field path not found")
	if len(path) > 1 {
		//if input == nil {
		//	return
		//}
		if inputType != mapType {
			return nil, pathError
		}
		if val, ok := input.(map[string]interface{})[path[0]]; ok {
			out, err = getVal(val, path[1:])
		} else {
			return nil, pathError
		}
	} else {
		if inputType == mapType {
			if val, ok := input.(map[string]interface{})[path[0]]; ok {
				out = val
			} else {
				return nil, pathError
			}
		} else {
			return nil, pathError
		}

		return
	}

	return
}

func jsonPrint(data interface{}) {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(b))
}
