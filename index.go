package flexdb

type Index []IndexItem

type IndexItem struct {
	Id    float64
	Value interface{}
	Doc   Doc
}
