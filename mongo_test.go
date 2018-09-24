package inosql

import (
	"fmt"
	"testing"

	"github.com/globalsign/mgo"
)

func TestMongoCreate(t *testing.T) {

	setter := NewSetter()
	setter.Set("name", "Alex")
	setter.Set("age", 16)
	setter.Set("rating", 9.2)
	setter.Set("friends", []string{"vasya", "petya"})
	setter.Set(
		"favorite_Films",
		map[string]interface{}{
			"Avatar":     9.5,
			"Green mile": 8.0,
		},
	)
	var cName = "someCollection"

	m, err := createMongoDB()
	if err != nil {
		t.Fatalf("DB creating error: %s", err.Error())
	}
	defer testDropDB(t, m.DB)

	val, err := m.Create(cName, setter)
	if err != nil {
		t.Fatalf("Doc create error %s", err.Error())
	}

	testResultCreating(t, val, m.DB.C(cName))

}

func testResultCreating(t *testing.T,
	val map[string]interface{},
	c *mgo.Collection,
) {
	var res []interface{}
	id, ok := val["_id"]
	if !ok {
		t.Fatalf("Havn't '_id' in returned after creating doc %+v", val)
	}
	var err = c.FindId(id).All(&res)
	if err != nil {
		t.Fatalf("Find after creating error: %s", err.Error())
	}

	if len(res) != 1 {
		t.Fatalf("Bad count elem in result, need 1, got %v", len(res))
	}

	var createdRes = fmt.Sprintf("%v", val)
	var dbQueryRes = fmt.Sprintf("%v", res[0])
	if len(createdRes) != len(dbQueryRes) {
		t.Fatalf("Bad result: need \n%s\ngot:\n%s", createdRes, dbQueryRes)
	}
}

func TestMongoSelect(t *testing.T) {
	var insertData = []interface{}{
		map[string]interface{}{
			"arr": []int{
				1,
				2,
				3,
			},
			"name": "Alexandr",
			"age":  16.5,
		},
		map[string]interface{}{
			"arr": []int{
				1,
				2,
				3,
				4,
				5,
			},
			"name": "nastya",
			"age":  17.5,
		},
		map[string]interface{}{
			"arr": []int{
				1,
				2,
			},
			"name": "Ahmed",
			"age":  16.5,
		},
		map[string]interface{}{
			"data": 1,
			"arr": []int{
				1,
			},
			"name": "Olya",
			"age":  14.0,
		},
	}
	var cName = "someCollectionName"

	m, err := createMongoDB()
	if err != nil {
		t.Fatalf("DB creating error: %s", err.Error())
	}
	defer testDropDB(t, m.DB)

	err = m.DB.C(cName).Insert(insertData...)

	if err != nil {
		t.Fatalf("Data insert error: %s", err.Error())
	}

	testEQ(t, m, cName)
	testNE(t, m, cName)
	testGT(t, m, cName)
	testGTE(t, m, cName)
	testLT(t, m, cName)
	testLTE(t, m, cName)
	testARRGTE(t, m, cName)
	testARRLTE(t, m, cName)
	testSTRGTE(t, m, cName)
	testSTRLTE(t, m, cName)
	testIN(t, m, cName)
	testNIN(t, m, cName)
	testSort(t, m, cName)
	testSkip(t, m, cName)
	testMaxDocs(t, m, cName)
}

func TestMongoUpdate(t *testing.T) {
	var insertData = []interface{}{
		map[string]interface{}{
			"arr": []int{
				1,
				2,
				3,
			},
			"name": "Alexandr",
			"age":  16.5,
		},
		map[string]interface{}{
			"arr": []int{
				1,
				2,
				3,
				4,
				5,
			},
			"name": "nastya",
			"age":  17.5,
		},
		map[string]interface{}{
			"arr": []int{
				1,
				2,
			},
			"name": "Ahmed",
			"age":  16.5,
		},
		map[string]interface{}{
			"data": 1,
			"arr": []int{
				1,
			},
			"name": "Olya",
			"age":  14.0,
		},
	}
	upd := NewSetter()
	upd.Set("age", 18)
	var cName = "someCollectionName"

	m, err := createMongoDB()
	if err != nil {
		t.Fatalf("DB creating error: %s", err.Error())
	}
	defer testDropDB(t, m.DB)

	err = m.DB.C(cName).Insert(insertData...)

	if err != nil {
		t.Fatalf("Data insert error: %s", err.Error())
	}

	s := NewSelecter()
	s.AddCompare("age", GTE, 16.5)
	s.SetSkipDocs(1)
	result, err := m.Update(cName, s, upd)
	if err != nil {
		t.Fatalf("Error of Update method %s\n ", err.Error())
	}
	if len(result) != 2 {
		t.Errorf("Need count 1, got %v", len(result))
	}
}

func TestMongoDelete(t *testing.T) {
	var insertData = []interface{}{
		map[string]interface{}{
			"arr": []int{
				1,
				2,
				3,
			},
			"name": "Alexandr",
			"age":  16.5,
		},
		map[string]interface{}{
			"arr": []int{
				1,
				2,
				3,
				4,
				5,
			},
			"name": "nastya",
			"age":  17.5,
		},
		map[string]interface{}{
			"arr": []int{
				1,
				2,
			},
			"name": "Ahmed",
			"age":  16.5,
		},
		map[string]interface{}{
			"data": 1,
			"arr": []int{
				1,
			},
			"name": "Olya",
			"age":  14.0,
		},
	}
	upd := NewSetter()
	upd.Set("age", 18)
	var cName = "someCollectionName"

	m, err := createMongoDB()
	if err != nil {
		t.Fatalf("DB creating error: %s", err.Error())
	}
	defer testDropDB(t, m.DB)

	err = m.DB.C(cName).Insert(insertData...)

	if err != nil {
		t.Fatalf("Data insert error: %s", err.Error())
	}

	s := NewSelecter()
	s.AddCompare("name", STRLTE, 20)
	s.SetSkipDocs(1)
	result, err := m.Delete(cName, s)
	if err != nil {
		t.Fatalf("Error of Delete method %s\n ", err.Error())
	}
	if result != 3 {
		t.Errorf("Need count 3, got %v", result)
	}
}

func testEQ(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", EQ, 16.5)
	result, err := db.Select(collection, s)

	if err != nil {
		t.Fatalf("testEQ: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 2 {
		t.Errorf("testEQ: Need count 2, got %v", len(result))
	}
}

func testNE(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", NE, 16.5)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Errorf("testNE: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 2 {
		t.Errorf("testNE: Need count 2, got %v", len(result))
	}
}

func testGT(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", GT, 16.5)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testGT: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 1 {
		t.Errorf("testGT: Need count 1, got %v", len(result))
	}
}

func testGTE(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", GTE, 16.5)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testGTE: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 3 {
		t.Errorf("testGTE: Need count 3, got %v,\n%+v", len(result), result)
	}
}

func testLT(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", LT, 16.5)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testLT: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 1 {
		t.Errorf("testLT: Need count 1, got %v", len(result))
	}
}

func testLTE(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", LTE, 16.5)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("Error of Select method %s\n ", err.Error())
	}

	if len(result) != 3 {
		t.Errorf("testLTE: Need count 3, got %v", len(result))
	}
}

func testARRGTE(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("arr", ARRGTE, 4)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testARRGTE: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 1 {
		t.Errorf("testARRGTE: Need count 1, got %v", len(result))
	}
}

func testARRLTE(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("arr", ARRLTE, 3)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testARRLTE: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 3 {
		t.Errorf("testARRLTE: Need count 3, got %v", len(result))
	}
}

func testSTRGTE(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("name", STRGTE, 4)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testSTRGTE: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 4 {
		t.Errorf("testSTRGTE: Need count 4, got %v", len(result))
	}
}

func testSTRLTE(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("name", STRLTE, 5)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testSTRLTE: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 2 {
		t.Errorf("testSTRLTE: Need count 2, got %v", len(result))
	}
}

func testIN(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	arr := []float64{2.2, 14, 17.5}
	s.AddCompare("age", IN, arr)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testIN: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 2 {
		t.Errorf("testIN: Need count 2, got %v", len(result))
	}
}
func testNIN(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	arr := []float64{2.2, 16.5, 17.5}
	s.AddCompare("age", NIN, arr)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testNIN: Error of Select method %s\n ", err.Error())
	}

	if len(result) != 1 {
		t.Errorf("testNIN: Need count 1, got %v", len(result))
	}
}

func testSort(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", GTE, 1.0)
	s.AddSortField("age", false)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testSort: Error of Select method %s\n ", err.Error())
	}
	i, _ := result[0]["age"].(float64)
	if i != 14 {
		t.Errorf("testSort: Need value 14, got %v", i)
	}
}

func testSkip(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddSortField("age", false)
	s.AddCompare("age", GTE, 1.0)
	s.SetSkipDocs(1)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testSkip: Error of Select method %s\n ", err.Error())
	}
	i, _ := result[0]["age"].(float64)
	if i != 16.5 {
		t.Errorf("testSkip: Need value 16.5, got %v", i)
	}
}

func testMaxDocs(t *testing.T, db Mongo, collection string) {
	s := NewSelecter()
	s.AddCompare("age", GTE, 1.0)
	s.SetMaxDocs(1)
	result, err := db.Select(collection, s)
	if err != nil {
		t.Fatalf("testSkip: Error of Select method %s\n ", err.Error())
	}
	if len(result) != 1 {
		t.Errorf("testSkip: Need value 1, got %v", len(result))
	}
}

func TestMongoSetUnique(t *testing.T) {
	var UniqVersions = []map[string][]string{
		{
			"c1": {
				"name",
				"surname",
				"age",
			},
			"c2": {
				"age",
			},
			"c3": {
				"username",
			},
			"c4": {},
		},
		{
			"c1": {
				"name",
				"surname",
			},
			"c2": {
				"age",
				"name",
			},
			"c3": {
				"user",
			},
			"c4": {
				"hello",
			},
		},
		{
			"c1": {
				"age",
				"name",
			},
			"c2": {
				"name",
				"surname",
			},
			"c3": {},
			"c4": {
				"hi",
			},
		},
	}

	mongo, err := createMongoDB()
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer testDropDB(t, mongo.DB)

	for _, version := range UniqVersions {
		err := mongo.SetUniqFields(version)
		if err != nil {
			t.Fatalf("Can't set unique fields: %v, error: %s", version, err.Error())
		}
	}

	lastVersion := UniqVersions[len(UniqVersions)-1]

	for collection, fields := range lastVersion {
		indexes, err := mongo.DB.C(collection).Indexes()
		if err != nil {
			t.Fatalf("Can't get indexes for collection %s, return error %s", collection, err.Error())
		}
		var indexFieldArr []string
		for _, i := range indexes {
			if len(i.Key) != 1 {
				t.Errorf("Bad len of index key arr, expected 1, got %v. array: %v", len(i.Key), i.Key)
				continue
			}
			indexFieldArr = append(indexFieldArr, i.Key[0])
		}

		//к сущестующим, ещё и индекс по _id
		if (len(fields) + 1) != len(indexFieldArr) {
			t.Fatalf("Bad count of uniq fields for collections %s. Expected %v, got %v  \n%+v", collection, len(fields)+1, len(indexFieldArr), indexFieldArr)
		}
		for _, field := range fields {
			var isHave = false
			for _, iField := range indexFieldArr {
				if field == iField {
					isHave = true
					continue
				}
			}
			if !isHave {
				t.Fatalf("Havn't field %s. Need arr %v, got %v", field, fields, indexFieldArr)
			}
		}
	}
}

func createMongoDB() (Mongo, error) {
	var m Mongo
	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		return m, err
	}
	m.DB = session.DB("mongoTest")
	return m, nil
}

func testDropDB(t *testing.T, m *mgo.Database) {
	err := m.DropDatabase()
	if err != nil {
		t.Errorf("not deleted db: %s", err.Error())
	}
}
