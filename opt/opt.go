package opt

const (
	ReadOpt   uint = 1
	UpdateOpt uint = 2
)

// TXOpt transaction operations.
type TXOpt struct {
	Type  uint
	Key   uint64
	Shard string
	Value interface{}
}

func (r *TXOpt) GetKey() (string, uint64) {
	return r.Shard, r.Key
}

func (r *TXOpt) GetValue() (interface{}, bool) {
	return r.Value, r.Type == UpdateOpt
}
