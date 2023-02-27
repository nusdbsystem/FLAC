package pages

type pgid uint64

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

const (
	metaPageFlags = 0x01
	validFlags    = 0x02
)

type page struct {
	id      pgid
	flags   uint16
	count   uint16
	headPtr uintptr
}

type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}
