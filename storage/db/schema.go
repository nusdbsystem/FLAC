package db

import "reflect"

// Schema stores the schema for one table.
type Schema struct {
	db         *DB
	table      *Table
	tupleCount int
	types      []reflect.Type
}

func (s *Schema) GetTupleSize() int {
	return s.tupleCount
}
