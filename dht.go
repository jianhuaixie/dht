package dht

import (
	"time"
	"flag"
	"net"
	"sync"
	"github.com/nettools"
	log "github.com/golang/glog"
	"crypto/rand"
	"strings"
	"expvar"
)

/* 消息类型：
	- query
	- response
	- error

	RPCs:
	- ping:查看节点能否ping通，如果能，就将其保存到路由表中。
	- find_node:查找节点，确保DHT路由表是能够正常使用的。
	- get_peers:节点反复询问DHT节点获取数据。
	- announce_peer:对外宣布，与某个节点连接并正在下载torrent。

	参考：
	http://www.bittorrent.org/beps/bep_0005.html
*/

// DHT节点的Config
type Config struct {
	Address string 					// 监听的IP address，如果留下空白，会自动选择一个。
	Port int 						// DHT节点会监听的UDP端口，如果是0，将会挑选一个随机端口。
	NumTargetPeers int 				// DHT将尝试为每个被搜索的infohash寻找的对等点。这可能会被转移到per-infohash选项。默认值:5。
	DHTRouters string 				// 用于引导网络的DHT路由器的分离列表。
	MaxNodes int 					// 在路由表中存储的最大节点数。默认值:100。
	CleanupPeriod time.Duration 	// 在网络中ping节点的频率，以确定它们是否可到达。默认值：15分钟。
	SaveRoutingTable bool 			// 如果True，节点将在启动时从磁盘读取路由表，并每隔几分钟保存磁盘上的路由表快照。默认值:True。
	SavePeriod time.Duration 		// 将路由表保存到磁盘的频率。默认值：5分钟。
	RateLimit int64 				// 每秒处理的最大数据包数量。如果是负数就取消。默认值:100。
	MaxInfoHashes int 				// MaxInfoHashes是我们应该保留一个对等列表的信息的数量的限制。
	// 如果这个和MaxInfoHashPeers没有改变，它应该消耗大约25 MB的RAM。更大的值有助于保持DHT网络的健康。默认值:2048。
	MaxInfoHashPeers int 			// MaxInfoHashPeers是每个infohash跟踪的对等点的数量限制。一个单独的对等接触通常会消耗6个字节。默认值:256。
	ClientPerMinuteLimit int 		//  ClientPerMinuteLimit 通过对抗垃圾客户端来进行保护。如果超过每分钟的数据包数量，请忽略它们的请求。默认值:50。
	ThrottlerTrackedClients int64 	// ThrottlerTrackedClients是客户端节流器所记得的主机的数量。LRU是用来跟踪最有趣的。默认值:1000。
	UDPProto string 				// UDP连接的协议，udp4 = IPv4  udp6 = IPv6
}

// 把Config填充上默认值
func NewConfig() *Config {
	return &Config{
		Address:"",
		Port:0,
		NumTargetPeers:"router.magnets.im:6881,router.bittorrent.com:6881,dht.transmissionbt.com:6881",
		MaxNodes:500,
		CleanupPeriod:15*time.Minute,
		SaveRoutingTable:true,
		SavePeriod:5*time.Minute,
		RateLimit:100,
		MaxInfoHashes:2048,
		MaxInfoHashPeers:256,
		ClientPerMinuteLimit:50,
		ThrottlerTrackedClients:1000,
		UDPProto:"udp4",
	}
}

var DefaultConfig = NewConfig()

// 将配置字段注册为命令行标志。如果c为nil，则使用DefaultConfig。
func RegisterFlags(c *Config) {
	if c == nil {
		c = DefaultConfig
	}
	flag.StringVar(&c.DHTRouters, "routers", c.DHTRouters,
		"Comma separated addresses of DHT routers used to bootstrap the DHT network.")
	flag.IntVar(&c.MaxNodes, "maxNodes", c.MaxNodes,
		"Maximum number of nodes to store in the routing table, in memory. This is the primary configuration for how noisy or aggressive this node should be. When the node starts, it will try to reach d.config.MaxNodes/2 as quick as possible, to form a healthy routing table.")
	flag.DurationVar(&c.CleanupPeriod, "cleanupPeriod", c.CleanupPeriod,
		"How often to ping nodes in the network to see if they are reachable.")
	flag.DurationVar(&c.SavePeriod, "savePeriod", c.SavePeriod,
		"How often to save the routing table to disk.")
	flag.Int64Var(&c.RateLimit, "rateLimit", c.RateLimit,
		"Maximum packets per second to be processed. Beyond this limit they are silently dropped. Set to -1 to disable rate limiting.")
}

const (
	minNodes = 16  // 尽量确保至少这些节点位于路由表中。
	secretRotatePeriod = 5 * time.Minute
)

type Logger interface {
	GetPeers(addr net.UDPAddr, queryID string, infoHash InfoHash)
}

// DHT 应该用New()来创建，能给torrent客户端提供一些DHT特征，例如发现新的对等节点让
// torrent下载，而不需要一个tracker。
type DHT struct {
	nodeId	string
	config 	Config
	routingTable *routingTable
	peerStore	*peerStore
	conn	*net.UDPConn
	Logger	Logger
	exploredNeighborhood	bool
	remoteNodeAcquaintance	chan string
	peersRequest	chan ihReq
	nodesRequest	chan ihReq
	pingRequest	chan *remoteNode
	portRequest	chan int
	stop	chan bool
	wg	sync.WaitGroup
	clientThrottle	*nettools.ClientThrottle
	store	*dhtStore
	tokenSecrets	[]string
	// Public channels:
	PeersRequestResults chan map[InfoHash][]string  // key = infohash , value = slice of peers
}

type ihReq struct {
	ih	InfoHash
	announce bool
}

// New()创建一个DHT node，如果config是nil，就用DefaultConfig填充，一旦创建DHT node之后，配置config就不能再更改
func New(config *Config) (node *DHT,err error) {
	if config == nil {
		config = DefaultConfig
	}
	cfg := *config
	node = &DHT{
		config:cfg,
		routingTable:newRoutingTable(),
		peerStore:newPeerStore(cfg.MaxInfoHashes,cfg.MaxInfoHashPeers),
		PeersRequestResults:make(chan map[InfoHash][]string, 1),
		stop:make(chan bool),
		exploredNeighborhood:false,
		// Buffer to avoid blocking on seeds
		remoteNodeAcquaintance:make(chan string,100),
		// Buffer to avoid deadlocks and blocking on sends
		peersRequest:make(chan ihReq,100),
		nodesRequest:make(chan ihReq,100),
		portRequest:    make(chan int),
		clientThrottle:nettools.NewThrottler(cfg.ClientPerMinuteLimit,cfg.ThrottlerTrackedClients),
		tokenSecrets:[]string{newTokenSecret(),newTokenSecret()},
	}
	c := openStore(cfg.Port,cfg.SaveRoutingTable)
	node.store = c
	if len(c.Id) != 20 {
		c.Id = randNodeId()
		log.V(4).Infof("Using a new random node ID: %x %d", c.Id, len(c.Id))
		saveStore(*c)
	}
	node.nodeId = string(c.Id)
	node.routingTable.nodeId = node.nodeId
	node.wg.Add(1)
	go func(){
		defer node.wg.Done()
		for addr,_ := range c.Remotes{
			node.AddNode(addr)
		}
	}()
	return
}

// AddNode通知DHT，它应该将一个新节点添加到它的路由表中。
// addr 是一个包含目标节点的 "host:port"UDP地址的字符串。
func (dht *DHT) AddNode(addr string) {
	dht.remoteNodeAcquaintance <- addr
}

func randNodeId() []byte {
	b := make([]byte,20)
	if _ ,err := rand.Read(b);err != nil {
		log.Fatalln("nodeId rand:",err)
	}
	return b
}
func newTokenSecret() string {
	b := make([]byte,5)
	if _,err := rand.Read(b);err!=nil{
		log.Warningf("DHT: failed to generate random newTokenSecret: %v", err)
	}
	return string(b)
}

// PeersRequest要求DHT为infoHash提供更多的对等点。如果连接的对等点正在积极地下载这个infohash，那么声明应该是正确的，
// 通常情况下是这这样的，除非这个DHT节点只是一个不下载torrents的路由器。
func (d *DHT) PeersRequest(ih string,announce bool){
	d.peersRequest <- ihReq{InfoHash(ih),announce}
	log.V(2).Infof("DHT: torrent client asking more peers for %x.", ih)
}

func (d *DHT) Stop(){
	close(d.stop)
	d.wg.Wait()
}

// Port() 返回给DHT的端口号，这在初始化带有端口0的DHT时非常有用，即自动端口分配，以便检索所使用的实际端口号。
func (d *DHT) Port() int{
	return <- d.portRequest
}

func (d *DHT) getPeers(infoHash InfoHash){
	closest := d.routingTable.lookupFiltered(infoHash)
	if len(closest) == 0 {
		for _,s := range strings.Split(d.config.DHTRouters,","){
			if s!=""{
				r,e := d.routingTable.getOrCreateNode("",s,d.config.UDPProto)
				if e == nil{
					d.getPeersFrom(r,infoHash)
				}
			}
		}
	}

}

func (d *DHT) getPeersFrom(r *remoteNode, ih InfoHash) {
	if r==nil{
		return
	}
	totalSentGetPeers.Add(1)
	ty := "get_peers"
	transId := r.newQuery(ty)
	if _,ok := r.pendingQueries[transId];ok{
		r.pendingQueries[transId].ih = ih
	}else{
		r.pendingQueries[transId] = &queryType{ih:ih}
	}
	queryArguments := map[string]interface{}{
		"id":        d.nodeId,
		"info_hash": ih,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	if log.V(3) {
		x := hashDistance(InfoHash(r.id), ih)
		log.V(3).Infof("DHT sending get_peers. nodeID: %x@%v, InfoHash: %x , distance: %x", r.id, r.address, ih, x)
	}
	r.lastSearchTime = time.Now()
	sendMsg(d.conn, r.address, query)
}



var (
	totalNodesReached = expvar.NewInt("totalNodesReached")
	totalGetPeersDupes = expvar.NewInt("totalGetPeersDupes")
	totalFindNodeDupes = expvar.NewInt("totalFindNodeDupes")
	totalSelfPromotions = expvar.NewInt("totalSelfPromotions")
	totalPeers = expvar.NewInt("totalPeers")
	totalSentPing = expvar.NewInt("totalSentPing")
	totalSentGetPeers = expvar.NewInt("totalSentGetPeers")
	totalSentFindNode = expvar.NewInt("totalSentFindNode")
	totalRecvGetPeers = expvar.NewInt("totalRecvGetPeers")
	totalRecvGetPeersReply = expvar.NewInt("totalRecvGetPeersReply")
	totalRecvPingReply = expvar.NewInt("totalRecvPingReply")
	totalRecvFindNode = expvar.NewInt("totalRecvFindNode")
	totalRecvFindNodeReply = expvar.NewInt("totalRecvFindNodeReply")
	totalPacketsFromBlockedHosts = expvar.NewInt("totalPacketsFromBlockedHosts")
	totalDroppedPackets = expvar.NewInt("totalDroppedPackets")
	totalRecv = expvar.NewInt("totalRecv")
)











