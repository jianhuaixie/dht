package dht

import (
	"time"
	"net"
	"github.com/nettools"
	"crypto/rand"
	"expvar"
	"github.com/youtube/vitess/go/vt/log"
	"strconv"
	"bytes"
	"github.com/jackpal/bencode-go"
)

// 每经过15秒就找一次节点
var (
	searchRetryPeriod = 15 * time.Second
	totalSent = expvar.NewInt("totalSent")
	totalReadBytes = expvar.NewInt("totalReadBytes")
	totalWrittenBytes = expvar.NewInt("totalWrittenBytes")
)

const (
	maxUDPPacketSize = 4096
	v4nodeContactLen = 26
	v6nodeContactLen = 38
	nodeIdLen = 20
)

// DHT 引擎所有的远程连接节点
type remoteNode struct {
	address net.UDPAddr
	addressBinaryFormat string
	id string
	lastQueryID int  						// lastQueryID需要在消费后自增。根据协议，应该是两个字母，但我使用0-255，虽然是当作字符串
	pendingQueries map[string]*queryType	// key: transaction ID
	pastQueries map[string]*queryType		// key: transaction ID
	reachable bool
	lastResponseTime time.Time
	lastSearchTime time.Time
	ActiveDownloads []string
}

type InfoHash string

type queryType struct {
	Type string
	ih InfoHash
	srcNode string
}

type getPeersResponse struct {
	Values []string "values"
	Id string "id"
	Nodes string "nodes"
	Nodes6 string "nodes6"
	Token string "token"
}

type answerType struct {
	Id string "id"
	Target string "target"
	InfoHash InfoHash "info_hash"
	Port int "port"
	Token string "token"
}

type responseType struct {
	T string "t"
	Y string "y"
	Q string "q"
	R getPeersResponse "r"
	E []string "e"
	A answerType "a"
}

type queryMessage struct {
	T string "t"
	Y string "y"
	Q string "q"
	A map[string]interface{} "a"
}

type replyMessage struct {
	T string "t"
	Y string "y"
	R map[string]interface{} "r"
}

type packetType struct{
	b []byte
	raddr net.UDPAddr
}

// 参数nodes是一个固定长度的包含任意连接的字符串
func parseNodesString(nodes string,proto string) (parsed map[string]string){
	var nodeContactLen int
	if proto == "udp4"{
		nodeContactLen = v4nodeContactLen
	}else if proto=="udp6"{
		nodeContactLen = v6nodeContactLen
	}else{
		return
	}
	parsed = make(map[string]string)
	if len(nodes)%nodeContactLen > 0 {
		log.V(3).Infof("DHT: len(NodeString) = %d, INVALID LENGTH, should be a multiple of %d", len(nodes), nodeContactLen)
		log.V(5).Infof("%T %#v\n", nodes, nodes)
		return
	}else{
		log.V(5).Infof("DHT: len(NodeString) = %d, had %d nodes, nodeContactLen=%d\n", len(nodes), len(nodes)/nodeContactLen, nodeContactLen)
	}
	for i := 0;i<len(nodes);i+=nodeContactLen{
		id := nodes[i:i+nodeIdLen]
		address := nettools.BinaryToDottedPort(nodes[i+nodeIdLen:i+nodeContactLen])
		parsed[id] = address
	}
	return
}

// newQuery 创建一个新的事务id并向r.pendingQueries添加一个条目。它不会为事务信息设置任何额外的信息，所以调用者必须处理它。
func (r *remoteNode) newQuery(transType string) (transId string){
	log.V(4).Infof("newQuery for %x, lastID %v", r.id, r.lastQueryID)
	r.lastQueryID = (r.lastQueryID+1)%256
	transId = strconv.Itoa(r.lastQueryID)
	log.V(4).Infof("... new id %v", r.lastQueryID)
	r.pendingQueries[transId] = &queryType{Type:transType}
	return
}

// 如果一个节点最近被联系过，也就是说最近被infohash请求过，返回true，如果每次请求都使用不同的infohash，就返回false
func (r *remoteNode) wasContactedRecently(ih InfoHash) bool {
	if len(r.pendingQueries) == 0 && len(r.pastQueries) == 0 {
		return  false
	}
	if !r.lastResponseTime.IsZero() && time.Since(r.lastResponseTime)>searchRetryPeriod {
		return false
	}
	for _,q := range r.pendingQueries {
		if q.ih == ih {
			return true
		}
	}
	if !r.lastSearchTime.IsZero() && time.Since(r.lastSearchTime) > searchRetryPeriod{
		return false
	}
	for _,q := range r.pastQueries{
		if q.ih == ih {
			return true
		}
	}
	return false
}

func bogusId(id string) bool {
	return len(id) != 20
}

func newRemoteNode(addr net.UDPAddr,id string) *remoteNode {
	return &remoteNode{
		address:addr,
		addressBinaryFormat:nettools.DottedPortToBinary(addr.String()),
		lastQueryID:newTransactionId(),
		id:id,
		reachable:false,
		pendingQueries:map[string]*queryType{},
		pastQueries:map[string]*queryType{},
	}
}
func newTransactionId() int {
	n,err := rand.Read(make([]byte,1))
	if err != nil {
		return time.Now().Second()
	}
	return n
}

func sendMsg(conn *net.UDPConn,raddr net.UDPAddr,query interface{}){
	totalSent.Add(1)
	var b bytes.Buffer
	if err := bencode.Marshal(&b,query);err != nil {
		return
	}
	if n,err := conn.WriteToUDP(b.Bytes(),&raddr);err != nil{
		log.V(3).Infof("DHT: node write failed to %+v, error=%s", raddr, err)
	}else{
		totalWrittenBytes.Add(int64(n))
	}
	return
}

func readResponse(p packetType) (response responseType,err error){
	defer func(){
		if x := recover();x!=nil{
			log.V(3).Infof("DHT: !!! Recovering from panic() after bencode.Unmarshal %q, %v", string(p.b), x)
		}
	}()
	if e2 := bencode.Unmarshal(bytes.NewBuffer(p.b),&response);e2==nil{
		err = nil
		return
	}else{
		log.V(3).Infof("DHT: unmarshal error, odd or partial data during UDP read? %v, err=%s", string(p.b), e2)
		return response, e2
	}
	return
}















