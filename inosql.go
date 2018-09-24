package inosql

type Operation int

const (
	//EQ ==
	EQ Operation = Operation(iota + 1)
	//NE !=
	NE
	//GT >
	GT
	//GTE >=
	GTE
	//LT <
	LT
	//LTE <=
	LTE
	//STRGTE string length >
	STRGTE
	//STRLTE string length <
	STRLTE
	//ARRGTE минимальная длинная массива
	ARRGTE
	//ARRLTE максимальнаяя длина массива
	ARRLTE
	//IN  определяет массив значений, одно из которых должно иметь поле документа
	IN
	//NIN определяет массив значений, которые не должно иметь поле документа
	NIN
)

func NewSetter() SetterBuilder {
	return &set{
		values: make(map[string]interface{}),
	}
}

func NewSelecter() SelectorBuilder {
	return &selectQuery{}
}

// DataBase интерфейс, который должна иметь наша обёртка над базой данных
type DataBase interface {
	Create(collectionName string, sets Setter) (map[string]interface{}, error)
	Select(collectionName string, sel Selector) ([]map[string]interface{}, error)
	Update(collectionName string, sel Selector, sets Setter) ([]map[string]interface{}, error)
	Delete(collectionName string, sel Selector) (int, error)
	SetUniqFields(map[string][]string) error
}

//Setter интерфейс для задания значения новой записи
type Setter interface {
	GetAllSets() map[string]interface{}
}

//SetterBuilder конструктор Settera
type SetterBuilder interface {
	Setter
	Set(fieldName string, val interface{})
}

//Selector интерфейс для задания выборок
type Selector interface {
	GetAllCompares() []Compare
	GetMaxDocs() int
	GetSkipDocs() int
	GetSortFields() []SortField
}

//SelectorBuilder конструктор Selectora
type SelectorBuilder interface {
	Selector
	AddCompare(string, Operation, interface{})
	SetMaxDocs(int)
	SetSkipDocs(int)
	AddSortField(string, bool)
}

//Compare интерфейс для получения сравнений
type Compare interface {
	GetValue() (string, Operation, interface{})
}

//SortField интерфейс для сортировки
type SortField interface {
	GetField() string
	IsInvers() bool
}

//set реализация интерфейса setterBuilder
type set struct {
	values map[string]interface{}
}

//selectQuery реализация интерфейса SelectorBuilder
type selectQuery struct {
	compares []Compare
	maxDocs  int
	skipDocs int
	sort     []SortField
}

//comparison реализация интерфейса Compare
type comparison struct {
	field string
	op    Operation
	value interface{}
}

//sortElem реализация интерфейса SortField
type sortElem struct {
	field    string
	isInvers bool
}

func (s *set) Set(fieldName string, val interface{}) {
	s.values[fieldName] = val
}
func (s *set) GetAllSets() map[string]interface{} {
	return s.values
}

func (s *selectQuery) AddCompare(name string, op Operation, value interface{}) {
	s.compares = append(s.compares, &comparison{
		field: name,
		op:    op,
		value: value,
	})
}
func (s *selectQuery) SetMaxDocs(max int) {
	s.maxDocs = max

}
func (s *selectQuery) SetSkipDocs(count int) {
	s.skipDocs = count
}
func (s *selectQuery) AddSortField(field string, isInvers bool) {
	s.sort = append(s.sort, &sortElem{
		field:    field,
		isInvers: isInvers,
	})
}
func (s *selectQuery) GetAllCompares() []Compare {
	return s.compares
}
func (s *selectQuery) GetMaxDocs() int {
	return s.maxDocs
}
func (s *selectQuery) GetSkipDocs() int {
	return s.skipDocs
}
func (s *selectQuery) GetSortFields() []SortField {
	return s.sort
}

func (c *comparison) GetValue() (string, Operation, interface{}) {
	return c.field, c.op, c.value
}

func (s *sortElem) GetField() string {
	return s.field
}
