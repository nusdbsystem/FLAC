package benchmark

import (
	"FLAC/configs"
	"FLAC/coordinator"
	mockkv2 "FLAC/storage/mockkv"
	"FLAC/utils"
	"math/rand"
	"strconv"
	"time"
)

const (
	//CustomReactionTime = 10 * time.Millisecond // Minimum reaction time for payment and new-order
	Newed         = 0
	Payed         = 1
	Delivered     = 2
	NWareHouse    = 3
	PayedSize     = 100
	NeedStockSize = 20
	AllOrderSize  = 2
)

func (c *TPCStmt) Init(pro string) {
	if configs.LocalTest {
		c.ca, c.co = coordinator.TpcTestKit()
	} else {
		c.ca = coordinator.RemoteTestkit("10.10.10.226:2001")
		c.co = nil
	}
	c.protocol = pro
	c.orderLine = 0
	c.OrderPoll = make([]*TPCOrder, 0)
	c.RandomizeRead()
}

type TPCOrderLine struct {
	Item  int
	Count int // Count is always 5 in the data.
	Stock int
}

type TPCOrder struct {
	Order    int
	Ware     int
	Customer int
	Items    []*TPCOrderLine
}

func random(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

func (c *TPCStmt) checkAndLoadStock(client *TPCClient, order *TPCOrder, res map[string]int) bool {
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		if res[configs.Hash(configs.OuAddress[v.Stock], key)] < 10 {
			if len(client.needStock.ToSlice()) < NeedStockSize {
				client.needStock.Add(v.Item*NWareHouse + v.Stock)
			}
		}
	}
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		if res[configs.Hash(configs.OuAddress[v.Stock], uint64(key))] < 5 {
			// TPCC Standard: no such false considered.
			return true
		}
	}
	return true
}

func (c *TPCStmt) payment(client *TPCClient, order *TPCOrder, parts []string) bool {
	configs.TPrintf("Handle payment begins from Client:" + strconv.Itoa(client.clientID))
	noTID := utils.GetTxnID()
	paWrite := coordinator.NewTX(noTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range parts {
		key := utils.TransTableItem("order", -1, order.Order)
		paWrite.AddUpdate(v, key, Payed)
	}
	ok := c.ca.Manager.SubmitTxn(paWrite, c.protocol, nil)
	for _, v := range order.Items {
		// TPCC standard: only one order payed, quick response.
		if len(client.payed.ToSlice()) < PayedSize {
			client.payed.Add(order.Order*NWareHouse + v.Stock)
		}
	}
	configs.TPrintf("Handle payment ends from Client:" + strconv.Itoa(client.clientID))
	return ok
}

func (c *TPCStmt) newOrder(client *TPCClient, order *TPCOrder, parts []string, stats *utils.Stat) bool {
	configs.TPrintf("Handle new order begins from Client:" + strconv.Itoa(client.clientID))
	noTID := utils.GetTxnID()
	configs.TPrintf("New order ts = %d Client:"+strconv.Itoa(client.clientID), noTID)
	defer configs.TimeTrack(time.Now(), "For New order", noTID)
	noRead := coordinator.NewTX(noTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		noRead.AddRead(configs.OuAddress[v.Stock], utils.TransTableItem("stock", v.Stock, v.Item))
	}
	//	val, ok := make(map[string]int), true
	val, ok := c.ca.Manager.PreRead(noRead)
	configs.TPrintf("New order read done from Client:" + strconv.Itoa(client.clientID))
	if !ok {
		configs.TxnPrint(noTID, "Failed for blocked PreRead")
		return false
	}
	noWrite := coordinator.NewTX(noTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		noWrite.AddUpdate(configs.OuAddress[v.Stock], key, val[configs.Hash(configs.OuAddress[v.Stock], key)]-5)
	}
	for _, v := range parts {
		key := utils.TransTableItem("order", -1, order.Order)
		noWrite.AddUpdate(v, key, Newed)
	}
	info := utils.NewInfo(len(noWrite.Participants))
	ok = c.ca.Manager.SubmitTxn(noWrite, c.protocol, info)
	stats.Append(info) // old TS
	if !ok {
		configs.TxnPrint(noTID, "Failed for commit")
	}
	if len(client.allOrderIDs.ToSlice()) < AllOrderSize {
		client.allOrderIDs.Add(order.Ware + order.Order*NWareHouse)
	}
	c.checkAndLoadStock(client, order, val)
	configs.TPrintf("Handle newOrder begins from Client:" + strconv.Itoa(client.clientID))
	return ok
}

// HandleOrder handle an Order from tpcc-generator
func (c *TPCStmt) HandleOrder(client *TPCClient, order *TPCOrder, stats *utils.Stat) bool {
	exist := make(map[int]bool)
	parts := make([]string, 0)
	// > 15% transactions are distributed.
	localStock := rand.Int()%100 > 15 // If processes the Order with local Stock.
	for i, v := range order.Items {
		if !localStock {
			v.Stock = i % NWareHouse
		} else {
			v.Stock = order.Ware
		}
		if !exist[v.Stock] {
			exist[v.Stock] = true
			parts = append(parts, configs.OuAddress[v.Stock])
		}
	}
	/// NewOrderTxn
	if c.newOrder(client, order, parts, stats) {
		configs.TPrintf("NewOrder Success")
	} else {
		configs.TPrintf("NewOrder Failed")
		return false
	}
	// TPCC standard: Reaction time + operating time for the Customer.
	//	time.Sleep(CustomReactionTime)
	c.payment(client, order, parts)
	return true
}

func (c *TPCStmt) stockLevel(order *TPCOrder, parts []string) bool {
	slTID := utils.GetTxnID()
	slRead := coordinator.NewTX(slTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		slRead.AddRead(configs.OuAddress[v.Stock], utils.TransTableItem("stock", v.Stock, v.Item))
	}
	val, ok := c.ca.Manager.PreRead(slRead)
	if !ok {
		configs.TPrintf("Failed for Stock read")
		return false
	}
	slWrite := coordinator.NewTX(slTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("stock", v.Stock, v.Item)
		slWrite.AddUpdate(configs.OuAddress[v.Stock], key, val[configs.Hash(configs.OuAddress[v.Stock], key)]+100)
	}
	ok = c.ca.Manager.SubmitTxn(slWrite, c.protocol, nil)
	return ok
}

// HandleStockLevel introduce Items with saved Stock requests.
func (c *TPCStmt) HandleStockLevel(client *TPCClient) {
	configs.TPrintf("Handle stock begins from Client:" + strconv.Itoa(client.clientID))
	exist := make(map[int]bool)
	parts := make([]string, 0)
	tmp := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	for _, v := range client.needStock.ToSlice() {
		ware := v.(int) % NWareHouse
		item := v.(int) / NWareHouse
		if !exist[ware] {
			exist[ware] = true
			parts = append(parts, configs.OuAddress[ware])
		}
		tmp.Items = append(tmp.Items, &TPCOrderLine{
			Stock: ware,
			Item:  item,
		})
	}
	configs.TPrintf("From Client:" + strconv.Itoa(client.clientID))
	c.stockLevel(tmp, parts)
	configs.TPrintf("Handle stock ends from Client:" + strconv.Itoa(client.clientID))
}

func (c *TPCStmt) delivery(order *TPCOrder, parts []string) bool {
	deTID := utils.GetTxnID()
	deWrite := coordinator.NewTX(deTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("order", v.Stock, v.Item)
		deWrite.AddUpdate(configs.OuAddress[v.Stock], key, Delivered)
	}
	ok := c.ca.Manager.SubmitTxn(deWrite, c.protocol, nil)
	return ok
}

// HandleDelivery deliver the payed Items.
func (c *TPCStmt) HandleDelivery(client *TPCClient) {
	configs.TPrintf("Begin handle delivery from Client:" + strconv.Itoa(client.clientID))
	exist := make(map[int]bool)
	parts := make([]string, 0)
	tmp := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	BatchSize := random(1, 10)
	// TPCC standard: pay for 1~10 random orders
	for i := 0; i < BatchSize; i++ {
		v := client.payed.Pop()
		if v == nil {
			break
		}
		ware := v.(int) % NWareHouse
		order := v.(int) / NWareHouse
		if !exist[ware] {
			exist[ware] = true
			parts = append(parts, configs.OuAddress[ware])
		}
		tmp.Items = append(tmp.Items, &TPCOrderLine{
			Stock: ware,
			Item:  order,
		})
	}
	c.delivery(tmp, parts)
	configs.TPrintf("Ends handle delivery from Client:" + strconv.Itoa(client.clientID))
}

func (c *TPCStmt) orderStatus(order *TPCOrder, parts []string) bool {
	osTID := utils.GetTxnID()
	osRead := coordinator.NewTX(osTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	for _, v := range order.Items {
		key := utils.TransTableItem("order", v.Stock, v.Item)
		osRead.AddRead(configs.OuAddress[v.Stock], key)
	}
	_, ok := c.ca.Manager.PreRead(osRead)
	osWrite := coordinator.NewTX(osTID, mockkv2.DefaultTimeOut, parts, c.ca.Manager)
	ok = ok && c.ca.Manager.SubmitTxn(osWrite, c.protocol, nil)
	return ok
}

// HandleOrderStatus check the status for one random orders.
func (c *TPCStmt) HandleOrderStatus(client *TPCClient) {
	configs.TPrintf("Handle order begins from Client:" + strconv.Itoa(client.clientID))
	exist := make(map[int]bool)
	parts := make([]string, 0)
	tmp := &TPCOrder{Items: make([]*TPCOrderLine, 0)}
	// Get a random order from a random warehouse.
	v := client.allOrderIDs.Pop()
	ware := v.(int) % NWareHouse
	order := v.(int) / NWareHouse
	if !exist[ware] {
		parts = append(parts, configs.OuAddress[ware])
		exist[ware] = true
	}
	tmp.Items = append(tmp.Items, &TPCOrderLine{
		Stock: ware,
		Item:  order,
	})
	c.orderStatus(tmp, parts)
	configs.TPrintf("Handle order ends from Client:" + strconv.Itoa(client.clientID))
}
