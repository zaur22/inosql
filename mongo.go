package inosql

import (
	"fmt"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/pkg/errors"
)

//Mongo реализация интерфейса Database
type Mongo struct {
	DB *mgo.Database
}

func (s *sortElem) IsInvers() bool {
	return s.isInvers
}

func (m *Mongo) Create(collectionName string, sets Setter) (map[string]interface{}, error) {
	doc := sets.GetAllSets()
	doc["_id"] = bson.NewObjectId()
	err := m.DB.C(collectionName).Insert(doc)
	if err != nil {
		errWrap := fmt.Sprintf("Can't create doc in database %s, collection: %s, params: %v",
			m.DB.Name,
			collectionName,
			doc,
		)
		return nil, errors.Wrap(err, errWrap)
	}
	return doc, nil
}

func (m *Mongo) Select(collectionName string, sel Selector) ([]map[string]interface{}, error) {

	var queryResult []interface{}

	q, err := m.createQueryMongo(collectionName, sel)

	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert data for select operation in collection %s, DB name %s",
			collectionName,
			m.DB.Name,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	err = q.All(&queryResult)

	if err != nil {
		var errWrap = fmt.Sprintf("Bad result of select query for db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	result, err := convertMongoResult(queryResult)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert result after select operation in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return nil, errors.Wrap(err, errWrap)
	}
	return result, nil
}

func (m *Mongo) Update(collectionName string, sel Selector, sets Setter) ([]map[string]interface{}, error) {

	var queryResult []interface{}

	q, err := m.createQueryMongo(collectionName, sel)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert data for select operation in collection %s, DB name %s",
			collectionName,
			m.DB.Name,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	err = q.All(&queryResult)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't update collection in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	result, err := convertMongoResult(queryResult)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert result after select operation in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	idArr, err := getIDArr(result)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't get id arr from result of select operation in db %s, collection %s, result %+v",
			m.DB.Name,
			collectionName,
			result,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	updCompares := NewSelecter()

	updCompares.AddCompare("_id", IN, idArr)

	updSel, err := convertComparesToMongo(updCompares.GetAllCompares())
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert data for select operation in collection %s, DB name %s",
			collectionName,
			m.DB.Name,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	var multiSet = map[string]interface{}{
		"$set": sets.GetAllSets(),
	}
	_, err = m.DB.C(collectionName).UpdateAll(updSel, multiSet)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't update collection in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	err = m.DB.C(collectionName).Find(updSel).All(&queryResult)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't update collection in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	result, err = convertMongoResult(queryResult)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert result after select operation in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	return result, nil
}

func (m *Mongo) Delete(collectionName string, sel Selector) (int, error) {

	var queryResult []interface{}

	q, err := m.createQueryMongo(collectionName, sel)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert data for select operation in collection %s, DB name %s",
			collectionName,
			m.DB.Name,
		)
		return 0, errors.Wrap(err, errWrap)
	}

	err = q.All(&queryResult)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't update collection in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return 0, errors.Wrap(err, errWrap)
	}

	result, err := convertMongoResult(queryResult)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert result after select operation in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return 0, errors.Wrap(err, errWrap)
	}

	idArr, err := getIDArr(result)

	if err != nil {
		var errWrap = fmt.Sprintf("Can't get id arr from result of select operation in db %s, collection %s, result %+v",
			m.DB.Name,
			collectionName,
			result,
		)
		return 0, errors.Wrap(err, errWrap)
	}

	delCompares := NewSelecter()

	delCompares.AddCompare("_id", IN, idArr)

	delSel, err := convertComparesToMongo(delCompares.GetAllCompares())
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert data for select operation in collection %s, DB name %s",
			collectionName,
			m.DB.Name,
		)
		return 0, errors.Wrap(err, errWrap)
	}

	info, err := m.DB.C(collectionName).RemoveAll(delSel)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't update collection in db %s, collection %s, query %+v",
			m.DB.Name,
			collectionName,
			q,
		)
		return 0, errors.Wrap(err, errWrap)
	}

	return info.Removed, nil
}

func (m *Mongo) SetUniqFields(uniqFields map[string][]string) error {
	for collection, newIndexFields := range uniqFields {
		var forRemoveIndex []string
		var forAddIndex []string
		oldIndexes, err := m.getCollectionIndexes(collection)
		if err != nil {
			return err
		}
		for _, fieldName := range newIndexFields {
			var isHave = false
			for _, oldIndex := range oldIndexes {
				if len(oldIndex.Key) != 1 {
					return fmt.Errorf("Bad count of index key, need 1, got %v", len(oldIndex.Key))
				}
				fName := oldIndex.Key[0]
				if fName == fieldName {
					isHave = true
					break
				}
			}
			if !isHave {
				forAddIndex = append(forAddIndex, fieldName)
			}
		}

		for _, oldIndex := range oldIndexes {
			var isHave = false
			var fName = oldIndex.Key[0]
			if fName == "_id" {
				continue
			}
			for _, fieldName := range newIndexFields {
				if fName == fieldName {
					isHave = true
					break
				}
			}
			if !isHave {
				forRemoveIndex = append(forRemoveIndex, fName)
			}
		}

		if len(forRemoveIndex) != 0 {
			err = m.removeIndexes(collection, forRemoveIndex)

			if err != nil {
				msg := fmt.Sprintf("Can't drop indexes %v", forRemoveIndex)
				return errors.Wrap(err, msg)
			}
		}

		for _, uniqField := range forAddIndex {
			var index = mgo.Index{
				Unique: true,
				Key:    []string{uniqField},
			}
			err = m.DB.C(collection).EnsureIndex(index)
			if err != nil {
				msg := fmt.Sprintf("Can't add index %v", index)
				return errors.Wrap(err, msg)
			}
		}
	}
	return nil
}

func getIDArr(sel []map[string]interface{}) ([]bson.ObjectId, error) {
	var res []bson.ObjectId
	for _, it := range sel {
		id, ok := it["_id"]
		if !ok {
			return nil, fmt.Errorf("Can't find '_id' key in map")
		}
		idStr, ok := id.(string)
		if !ok {
			return nil, fmt.Errorf("Bad type of id, excpected string, got %T", id)
		}
		res = append(res, bson.ObjectIdHex(idStr))
	}
	return res, nil
}

func (m *Mongo) createQueryMongo(collectionName string, sel Selector) (*mgo.Query, error) {
	compares := sel.GetAllCompares()
	s, err := convertComparesToMongo(compares)
	if err != nil {
		return nil, err
	}

	q := m.DB.C(collectionName).Find(s)

	if len(sel.GetSortFields()) > 0 {
		var fields []string
		for _, field := range sel.GetSortFields() {
			if field.IsInvers() {
				fields = append(fields, "-"+field.GetField())
			} else {
				fields = append(fields, field.GetField())
			}
		}
		q = q.Sort(fields...)
	}

	if sel.GetSkipDocs() > 0 {
		q = q.Skip(sel.GetSkipDocs())
	}
	if sel.GetMaxDocs() > 0 {
		q = q.Limit(sel.GetMaxDocs())
	}
	return q, nil
}

func convertMongoResult(queryResult interface{}) ([]map[string]interface{}, error) {
	arrRes, ok := queryResult.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Unexpected type of res, expected []interface{}, got %T, value %+v", queryResult, queryResult)
	}

	var result []map[string]interface{}

	for _, item := range arrRes {
		val, ok := item.(bson.M)
		if !ok {
			return nil, fmt.Errorf("Can't convert interface to bson: %#v", item)
		}

		id, ok := val["_id"].(bson.ObjectId)
		if !ok {
			return nil, fmt.Errorf("Can't convert interface to ObjectId %+v", val["_id"])
		}
		val["_id"] = id.Hex()
		result = append(result, val)
	}
	return result, nil
}

func convertComparesToMongo(compares []Compare) (interface{}, error) {
	var andExpression []map[string]interface{}
	for _, c := range compares {

		name, operation, value := c.GetValue()

		var expr = map[string]interface{}{}

		switch operation {
		case EQ:
			expr["$eq"] = []interface{}{
				"$" + name,
				value,
			}
		case NE:
			expr["$ne"] = []interface{}{
				"$" + name,
				value,
			}
		case GT:
			expr["$gt"] = []interface{}{
				"$" + name,
				value,
			}
		case LT:
			expr["$lt"] = []interface{}{
				"$" + name,
				value,
			}
		case GTE:
			expr["$gte"] = []interface{}{
				"$" + name,
				value,
			}
		case LTE:
			expr["$lte"] = []interface{}{
				"$" + name,
				value,
			}
		case STRGTE:
			expr["$gte"] = []interface{}{
				map[string]interface{}{
					"$strLenCP": "$" + name,
				},
				value,
			}
		case STRLTE:
			expr["$lte"] = []interface{}{
				map[string]interface{}{
					"$strLenCP": "$" + name,
				},
				value,
			}
		case ARRGTE:
			expr["$gte"] = []interface{}{
				map[string]interface{}{
					"$size": "$" + name,
				},
				value,
			}
		case ARRLTE:
			expr["$lte"] = []interface{}{
				map[string]interface{}{
					"$size": "$" + name,
				},
				value,
			}
		case IN:
			expr["$in"] = []interface{}{
				"$" + name,
				value,
			}
		case NIN:
			expr["$not"] = map[string]interface{}{
				"$in": []interface{}{
					"$" + name,
					value,
				},
			}
		default:
			return nil, fmt.Errorf("Underfined constant for select operation %v", operation)
		}
		andExpression = append(andExpression, expr)
	}
	result := map[string]interface{}{}
	result["$expr"] = map[string]interface{}{
		"$and": andExpression,
	}
	return result, nil
}

func (m *Mongo) getCollectionIndexes(collection string) ([]mgo.Index, error) {
	var indexes []mgo.Index
	var result interface{}
	var isHave = false
	var err error
	collectionNames, err := m.DB.CollectionNames()
	if err != nil {
		return nil, err
	}

	for _, name := range collectionNames {
		if name == collection {
			isHave = true
			break
		}
	}

	if isHave {
		indexes, err = m.DB.C(collection).Indexes()
		if err != nil {
			msg := fmt.Sprintf("can't get collection %s indexes", collection)
			return nil, errors.Wrap(err, msg)
		}
	} else {
		err = m.DB.Run(bson.D{{"create", collection}}, result)
		if err != nil {
			msg := fmt.Sprintf("can't create collection %s", collection)
			return nil, errors.Wrap(err, msg)
		}
	}

	return indexes, nil
}

func (m *Mongo) removeIndexes(collection string, forRemoveIndex []string) error {
	indexes, err := m.DB.C(collection).Indexes()
	if err != nil {
		msg := fmt.Sprintf("Can't get indexes from collection %s", collection)
		return errors.Wrap(err, msg)
	}
	for _, index := range indexes {
		if len(index.Key) != 1 {
			return fmt.Errorf("Unexpected len of index keys, excpected 1, got %v, index: %+v", len(index.Key), index)
		}
		for _, fieldName := range forRemoveIndex {
			if fieldName == index.Key[0] {
				err = m.DB.C(collection).DropIndexName(index.Name)
				if err != nil {
					msg := fmt.Sprintf("Can't remove index %s from collection %s", index.Name, collection)
					return errors.Wrap(err, msg)
				}
				break
			}
		}
	}
	return nil
}
