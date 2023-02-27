package db

import (
	"FLAC/configs"
	"FLAC/storage/concurrency"
	"sync/atomic"
)

// Table database table.
// TODO:
type Table struct {
	tableName string
	tableSize uint32
	schema    *Schema
}

func (tb *Table) Size() int {
	return (int)(tb.tableSize)
}

func (tb *Table) Schema() *Schema {
	return tb.schema
}

func (tb *Table) Name() string {
	return tb.tableName
}

func NewTable(_schema *Schema) *Table {
	return &Table{
		tableSize: 0,
		schema:    _schema,
	}
}

func (tb *Table) NewRow(row **RowRecord, bucketID uint64, rowID uint64) error {
	atomic.AddUint32(&tb.tableSize, 1)
	*row = &RowRecord{table: tb, primaryKey: rowID, bucketID: bucketID}
	(*row).data = make([]interface{}, tb.schema.tupleCount)
	return nil
}

// RowRecord stores the data for one table row record.
type RowRecord struct {
	primaryKey uint64
	bucketID   uint64
	data       []interface{} // TODO: refine this part.
	table      *Table
	ccManager  *cc.BaseCC
}

func (r *RowRecord) Table() *Table {
	return r.table
}

func (r *RowRecord) ReForm(size int) {
	r.data = make([]interface{}, size)
}

func (r *RowRecord) GetTupleSize() int {
	configs.Assert(len(r.data) == r.Table().Schema().GetTupleSize(), "row record get tuple: incorrect size")
	return len(r.data)
}

func (r *RowRecord) Copy(_r *RowRecord) {
	configs.Assert(len(r.data) == len(_r.data), "row record copy: unmatched length")
	copy(r.data, _r.data)
}

func (r *RowRecord) Free() {
}
