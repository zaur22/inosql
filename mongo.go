package inosql

import (
	"fmt"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/pkg/errors"
)

type Mongo struct {
	DB mgo.Database
}

func (m *Mongo) Create(collectionName string, sets Setter) (map[string]interface{}, error) {
	doc := sets.getAllSets()
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
	compares := sel.getAllCompares()
	s, err := convertComparesToMongo(compares)
	if err != nil {
		var errWrap = fmt.Sprintf("Can't convert data for select operation in collection %s",
			collectionName,
		)
		return nil, errors.Wrap(err, errWrap)
	}

	q := m.DB.C(collectionName).Find(s)

	if sel.GetSortFields

	if sel.GetSkipDocs() > 0 {
		q = q.Skip(sel.GetSkipDocs())
	}
	if sel.GetMaxDocs() > 0{
		q = q.
	}

}

func convertComparesToMongo(compares []Compare) (interface{}, error) {
	var res = map[string]map[string]interface{}{}
	for _, c := range compares {
		name, operation, value := c.getValue()
		_, ok := res[name]
		if !ok {
			res[name] = make(map[string]interface{})
		}
		switch operation {
		case EQ:
			res[name]["$eq"] = value
		case NE:
			res[name]["$ne"] = value
		case GT:
			res[name]["$gt"] = value
		case LT:
			res[name]["$lt"] = value
		case GTE:
			res[name]["$eq"] = value
		case LONGER:
			res[name]["$size"] = map[string]interface{}{
				"$gt": value,
			}
		case SHORTER:
			res[name]["$size"] = map[string]interface{}{
				"$lt": value,
			}
		case IN:
			res[name]["$in"] = value
		case NIN:
			res[name]["$nin"] = value
		}
	}

	return res, nil
}
