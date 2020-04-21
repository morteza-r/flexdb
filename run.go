package flexdb

func (db *Database) AddQuery(q Query) (result interface{}, err error) {
	// add single doc
	err = db.Add(&q.Table, q.Doc)
	result = q.Doc

	return
}

func (db *Database) AllQuery(q Query) (result interface{}, err error) {
	// all docs need
	var docs []Doc
	err = db.All(q, &docs)
	if err != nil {
		return
	}
	result = docs

	return
}

func (db *Database) GetQuery(q Query) (result interface{}, err error) {
	// where exist
	if len(q.Where) != 0 {
		filteredDocs, err := db.WhereQuery(q)
		if err != nil {
			return result, err
		}
		if len(filteredDocs) > 0 {
			result = &filteredDocs[0]
		}

		//fmt.Println(">> 1, result, err", result, err)
		return result, err
	}

	//single doc
	_, err = q.Doc.GetId()
	if err == nil {
		if !q.Doc.IsEmpty() {
			err = db.Get(&q.Table, q.Doc)
			if err != nil {
				return nil, err
			}
			result = q.Doc

			//fmt.Println(">> 2, result, err", result, err)
			return
		}
	}

	//// all docs need
	//var docs []Doc
	//err = db.All(q, &docs)
	//if err != nil {
	//	return
	//}
	//if len(docs) > 0 {
	//	result = &docs[0]
	//}

	return
}

func (db *Database) MGetQuery(q Query) (result interface{}, err error) {
	// where exist
	if len(q.Where) != 0 {
		filteredDocs, err := db.WhereQuery(q)
		if err != nil {
			return result, err
		}
		result = filteredDocs

		return result, err
	}

	// all docs need
	var docs []Doc
	err = db.All(q, &docs)
	if err != nil {
		return
	}
	result = docs

	return
}

func (db *Database) ExistsQuery(q Query) (result interface{}, err error) {
	result = false
	// where exist
	if len(q.Where) != 0 {
		filteredDocs, err := db.WhereQuery(q)
		if err != nil {
			return false, err
		}
		if len(filteredDocs) > 0 {
			return true, nil
		} else {
			return false, nil
		}
	}

	// single doc
	if !q.Doc.IsEmpty() {
		err = db.Get(&q.Table, q.Doc)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	return
}

func (db *Database) ReplaceQuery(q Query) (result interface{}, err error) {
	// single replace
	if len(q.Where) == 0 {
		err = db.Replace(&q.Table, q.Doc)
		if err != nil {
			return
		}
		result = q.Doc

		return
	}

	// multiple - where exist
	filteredDocs, err := db.WhereQuery(q)
	if err != nil {
		return
	}
	var resultDocs []Doc
	for _, doc := range filteredDocs {
		tempDoc := NewDoc()
		id, _ := doc.GetId()
		tempDoc.FillFields(q.Doc.Fields)
		_ = tempDoc.SetId(id)
		err = db.Replace(&q.Table, tempDoc)
		if err == nil {
			resultDocs = append(resultDocs, *tempDoc)
		}
	}
	result = resultDocs

	return
}

func (db *Database) UpdateQuery(q Query) (result interface{}, err error) {
	// single update
	if len(q.Where) == 0 {
		err = db.Update(&q.Table, q.Doc)
		if err != nil {
			return
		}
		result = q.Doc
	}

	// multiple update - where
	filteredDocs, err := db.WhereQuery(q)
	if err != nil {
		return
	}
	var resultDocs []Doc
	for _, doc := range filteredDocs {
		tempDoc := NewDoc()
		id, _ := doc.GetId()
		tempDoc.FillFields(q.Doc.Fields)
		_ = tempDoc.SetId(id)
		err = db.Update(&q.Table, tempDoc)
		if err == nil {
			resultDocs = append(resultDocs, *tempDoc)
		}
	}
	result = resultDocs

	return
}

func (db *Database) DeleteQuery(q Query) (result interface{}, err error) {
	// single delete
	if len(q.Where) == 0 {
		err = db.Delete(&q.Table, q.Doc)
		if err != nil {
			return
		}
		result = q.Doc

		return
	}

	// multiple delete - where
	filteredDocs, err := db.WhereQuery(q)
	if err != nil {
		return
	}
	var resultDocs []Doc
	for _, doc := range filteredDocs {
		tempDoc := NewDoc()
		id, _ := doc.GetId()
		_ = tempDoc.SetId(id)
		err = db.Delete(&q.Table, tempDoc)
		if err == nil {
			resultDocs = append(resultDocs, *tempDoc)
		}
	}
	result = resultDocs

	return
}

func (db *Database) WhereQuery(q Query) (filteredDocs []Doc, err error) {
	idList, err := db.Where(&q.Table, &q.Where, &q.WhereType)
	if err != nil {

		return
	}
	sortedIdList, err := db.Order(&q.Table, idList, &q.Order.Field, &q.Order.Type, &q.Limit)
	if err != nil {

		return
	}
	err = db.MultiGet(&q.Table, sortedIdList, &filteredDocs)
	if err != nil {

		return
	}

	return
}
