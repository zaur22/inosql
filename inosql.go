package inosql

type Operation int

const (
	//EQ ==
	EQ Operation = Operation(iota + 1)
	//NOTEQ !=
	NE
	//MORE >
	GT
	//MOREEQ >=
	GTE
	//LESSER <
	LT
	//LESSEREQ <=
	LTE
	//LONGER минимальная длинная массива
	LONGER
	//SHORTER максимальнаяя длина массива
	SHORTER
	//IN  определяет массив значений, одно из которых должно иметь поле документа
	IN
	//NIN определяет массив значений, которые не должно иметь поле документа
	NIN
)

// DataBase интерфейс, который должна иметь наша обёртка над базой данных
type DataBase interface {
	Create(collectionName string, sets Setter) (map[string]interface{}, error)
	Select(collectionName string, sel Selector) ([]map[string]interface{}, error)
	Update(collectionName string, sel Selector, sets Setter) ([]map[string]interface{}, error)
	Delete(collectionName string, sel Selector) (int, error)
}

//Setter интерфейс для задания значения новой записи
type Setter interface {
	getAllSets() map[string]interface{}
}

//SetterBuilder конструктор Settera
type SetterBuilder interface {
	Setter
	Set(fieldName string, val interface{})
}

//Selector интерфейс для задания выборок
type Selector interface {
	getAllCompares() []Compare
	GetMaxDocs() int
	GetSkipDocs() int
	GetSortFields() []map[string]bool
}

//SelectorBuilder конструктор Selectora
type SelectorBuilder interface {
	Selector
	AddCompare(string, Operation, interface{})
	SetMaxDocs(int)
	SetSkipDocs(int)
	SortByFileds([]map[string]bool)
}

//Compare интерфейс для получения сравнений
type Compare interface {
	getValue() (string, Operation, interface{})
}
