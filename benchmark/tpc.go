package benchmark

import (
	"FLAC/configs"
	"FLAC/coordinator"
	"FLAC/participant"
	"FLAC/utils"
	"encoding/csv"
	"encoding/json"
	"fmt"
	set "github.com/deckarep/golang-set"
	"github.com/jinzhu/copier"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type TPCClient struct {
	clientID    int
	needStock   set.Set
	payed       set.Set
	allOrderIDs set.Set
	pop         int
	from        *TPCStmt
}

func NewTPCClient(id int) *TPCClient {
	c := &TPCClient{}
	c.clientID = id
	c.needStock = set.NewSet()
	c.payed = set.NewSet() // <= 1000
	c.allOrderIDs = set.NewSet()
	return c
}

type TPCStmt struct {
	ca        *coordinator.Context
	co        []*participant.Context
	protocol  string
	orderLine int32
	OrderPoll []*TPCOrder
	stat      *utils.Stat
	stop      int32
}

func (c *TPCStmt) ReadOrder(o *csv.Reader, ol *csv.Reader) *TPCOrder {
	row, err := o.Read()
	if err == io.EOF {
		return nil
	}
	configs.CheckError(err)

	res := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	res.Order, err = strconv.Atoi(row[0])
	configs.CheckError(err)
	res.Ware, err = strconv.Atoi(row[1])
	configs.CheckError(err)
	res.Ware--
	res.Customer, err = strconv.Atoi(row[2])
	configs.CheckError(err)
	count, err := strconv.Atoi(row[3])
	configs.CheckError(err)
	for i := 0; i < count; i++ {
		row, err = ol.Read()
		if err == io.EOF {
			return nil
		}
		configs.CheckError(err)
		item, err := strconv.Atoi(row[0])
		configs.CheckError(err)
		amount, err := strconv.Atoi(row[1])
		configs.CheckError(err)
		if i < 5 {
			res.Items = append(res.Items, &TPCOrderLine{
				Item:  item,
				Count: amount})
		}
	}
	return res
}

func JPrint(v interface{}) {
	byt, _ := json.Marshal(v)
	fmt.Println(string(byt))
}

func (c *TPCStmt) GetOrder() *TPCOrder {
	return c.OrderPoll[random(0+100, len(c.OrderPoll)-100)]
}

func (c *TPCStmt) RandomizeRead() {
	file_order, err := os.Open("./data/new_order.csv")
	if err != nil {
		file_order, err = os.Open("./benchmark/data/new_order.csv")
	}
	configs.CheckError(err)
	defer file_order.Close()
	order_line, err := os.Open("./data/order_line.csv")
	if err != nil {
		order_line, err = os.Open("./benchmark/data/order_line.csv")
	}
	configs.CheckError(err)
	defer order_line.Close()

	order := csv.NewReader(file_order)
	lines := csv.NewReader(order_line)
	for {
		tp := c.ReadOrder(order, lines)
		if tp == nil {
			break
		} else {
			c.OrderPoll = append(c.OrderPoll, tp)
		}
	}
	configs.TPrintf("Order all loaded")
	rand.Shuffle(len(c.OrderPoll), func(i, j int) { c.OrderPoll[i], c.OrderPoll[j] = c.OrderPoll[j], c.OrderPoll[i] })
}

func (stmt *TPCStmt) Stop() {
	configs.LPrintf("Client stop !!!!!")
	stmt.ca.Close()
	atomic.StoreInt32(&stmt.stop, 1)
	if stmt.co == nil {
		return
	}
	if stmt.co == nil {
		return
	}
	for _, v := range stmt.co {
		v.Close()
	}
}

func (stmt *TPCStmt) Stopped() bool {
	return atomic.LoadInt32(&stmt.stop) != 0
}

func (stmt *TPCStmt) TPCClient(id int) {
	client := NewTPCClient(id)
	for !stmt.Stopped() {
		for count := 0; count < 20 && !stmt.Stopped(); count++ {
			tmp := &TPCOrder{}
			configs.CheckError(copier.CopyWithOption(&tmp, stmt.GetOrder(), copier.Option{DeepCopy: true}))
			if !stmt.Stopped() {
				stmt.HandleOrder(client, tmp, stmt.stat)
			}
			if count%20 == 10 && !stmt.Stopped() {
				stmt.HandleOrderStatus(client)
			}
		}
		if !stmt.Stopped() {
			stmt.HandleDelivery(client)
			stmt.HandleStockLevel(client)
		}
	}
}

func (stmt *TPCStmt) RunTPC() {
	//	atomic.StoreInt32(&stmt.stop, 0)
	stmt.stat = utils.NewStat()
	for i := 0; i < configs.ClientRoutineNumber; i++ {
		go stmt.TPCClient(i)
	}
	configs.TPrintf("All clients Started")
	time.Sleep(configs.WarmUpTime)
	stmt.stat.Clear()
	configs.TPrintf("Warm up done!!!! :)")
	time.Sleep(8 * time.Second)
	stmt.stat.Log()
	stmt.stat.Clear()
}

func (stmt *TPCStmt) TPCC_Test(ca *coordinator.Context) {
	rand.Seed(1234)
	stmt.Init(configs.SelectedACP)
	stmt.RunTPC()
}
