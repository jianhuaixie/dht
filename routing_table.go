package dht

import (
	"net"
	"fmt"
	"github.com/youtube/vitess/go/vt/log"
	"expvar"
	"github.com/nettools"
	"time"
)

type routingTable struct {
	*nTree
	addresses map[string]*remoteNode 	// 地址是UDP地址的map  host:port格式表示一堆remoteNodes。
	// 之所以使用字符串，是因为不可能使用net.UDPAddr创建映射。
	nodeId string					// 节点自己本身的ID
	boundaryNode *remoteNode		// 跟NodeID距离最远的路由表中的某个节点
	proximity int					// NodeID跟boundaryNode之间的距离有多少个前缀位
}

// 构建一个路由表，二叉树是空的，自己NodeID是空的，其他的都是空的
func newRoutingTable() *routingTable{
	return &routingTable{
		&nTree{},
		make(map[string]*remoteNode),
		"",
		nil,
		0,
	}
}

// hostPortToNode根据指定的hostPort规范在路由表中找到一个节点，它应该是一个UDP地址，形式为“host:port”。
func (r *routingTable) hostPortToNode(hostPort string,port string) (node *remoteNode,addr string,existed bool,err error) {
	if hostPort == "" {
		panic("programing error:hostPortToNode received a nil hostPort")
	}
	address,err := net.ResolveIPAddr(port,hostPort)
	if err != nil {
		return nil,"",false,err
	}
	if address.String() == ""{
		return nil,"",false,fmt.Errorf("programming error: address resolution for hostPortToNode returned an empty string")
	}
	n,existed := r.addresses[address.String()]
	if existed && n == nil {
		return nil, "", false, fmt.Errorf("programming error: hostPortToNode found nil node in address table")
	}
	return n,address.String(),existed,nil
}

// 统计一下节点的路由表中有多少个节点
func (r *routingTable) length() int {
	return len(r.addresses)
}

func (r *routingTable) reachableNodes() (tbl map[string][]byte) {
	tbl = make(map[string][]byte)
	for addr ,remoteNode := range r.addresses {
		if addr == "" {
			log.V(3).Infof("reachableNodes: found empty address for node %x.", r.id)
			continue
		}
		if remoteNode.reachable && len(remoteNode.id) == 20 {
			tbl[addr] = []byte(remoteNode.id)
		}
	}
	hexId := fmt.Sprintf("%x", r.nodeId)
	v := new(expvar.Int)
	v.Set(int64(len(tbl)))
	reachableNodes.Set(hexId,v) // 将远程可触达的node塞进去
	return
}

func (r *routingTable) numNodes() int {
	return len(r.addresses)
}

func isValidAddr(addr string) bool {
	if addr == ""{
		return false
	}
	if h,p,err := net.SplitHostPort(addr);h==""||p==""||err!=nil{
		return false
	}
	return true
}

// 通过设置正确的infohash id来更新这个节点的现有路由条目。如果没有找到节点，就会出现错误。
func (r *routingTable) update(node *remoteNode,proto string) error {
	_,addr,exited,err := r.hostPortToNode(node.address.String(),proto)
	if err != nil {
		return err
	}
	if !isValidAddr(addr){
		return fmt.Errorf("routingTable.update received an invalid address %v", addr)
	}
	if !exited{
		return fmt.Errorf("node missing from the routing table: %v", node.address.String())
	}
	if node.id != "" {
		r.nTree.insert(node)
		totalNodes.Add(1)
		r.addresses[addr].id = node.id
	}
	return nil
}

// 将所提供的节点插入到路由表中。如果另一个节点已经存在这个地址，则会出现错误。
func (r *routingTable) insert(node *remoteNode,proto string) error {
	if node.address.Port == 0 {
		return fmt.Errorf("routingTable.insert() got a node with Port=0")
	}
	if node.address.IP.IsUnspecified() {
		return fmt.Errorf("routingTable.insert() got a node with a non-specified IP address")
	}
	_,addr,existed,err := r.hostPortToNode(node.address.String(),proto)
	if err != nil {
		return err
	}
	if !isValidAddr(addr){
		return fmt.Errorf("routingTable.insert received an invalid address %v", addr)
	}
	if existed {
		return nil
	}
	r.addresses[addr] = node
	if !bogusId(node.id) {
		r.nTree.insert(node)
		totalNodes.Add(1)
	}
	return nil
}

// getOrCreateNode为hostPort返回一个节点，它可以是一个IP:port或Host:port，如果可能的话，它将被解析。
// 最好返回已经在路由表中的条目，但是创建一个新的条目，因此是幂等的。
func (r *routingTable) getOrCreateNode(id string,hostPort string,proto string) (node *remoteNode,err error) {
	node,addr,existed,err := r.hostPortToNode(hostPort,proto)
	if err != nil {
		return  nil,err
	}
	if existed {
		return node,nil
	}
	udpAddr ,err := net.ResolveUDPAddr(proto,addr)
	if err != nil {
		return nil,err
	}
	node = newRemoteNode(*udpAddr,id)
	return node,r.insert(node,proto)
}

func (r *routingTable) kill(n *remoteNode,p *peerStore) {
	delete(r.addresses,n.address.String())
	r.nTree.cut(InfoHash(n.id),0)
	totalKilledNodes.Add(1)
	if r.boundaryNode != nil && n.id == r.boundaryNode.id {
		r.resetNeighborhoodBoundary()
	}
	p.killContact(nettools.BinaryToDottedPort(n.addressBinaryFormat))
}

func (r *routingTable) resetNeighborhoodBoundary() {
	r.proximity = 0
	// 试着在邻居节点中找到一个遥远的地方，并把它作为邻居节点中最遥远的节点来推广。
	neighbors := r.lookup(InfoHash(r.nodeId))
	if len(neighbors) > 0 {
		r.boundaryNode = neighbors[len(neighbors)-1]
		r.proximity = commonBits(r.nodeId,r.boundaryNode.id)
	}
}

func (r *routingTable) cleanup(cleanupPeriod time.Duration,p *peerStore) (needPing []*remoteNode) {
	needPing = make([]*remoteNode,0,10)
	t0 := time.Now()
	for addr ,n := range r.addresses{
		if addr != n.address.String(){
			log.V(3).Infof("cleanup: node address mismatches: %v != %v. Deleting node", addr, n.address.String())
			r.kill(n, p)
			continue
		}
		if addr == "" {
			log.V(3).Infof("cleanup: found empty address for node %x. Deleting node", n.id)
			r.kill(n, p)
			continue
		}
		if n.reachable{
			if len(n.pendingQueries) == 0{
				goto PING
			}
			// remotenode能可达，但上次回应的时间点太旧了，还是干掉好了
			if time.Since(n.lastResponseTime) > cleanupPeriod*2+(cleanupPeriod/15){
				log.V(4).Infof("DHT: Old node seen %v ago. Deleting", time.Since(n.lastResponseTime))
				r.kill(n, p)
				continue
			}
			// 最近才看到，就不需要再ping
			if time.Since(n.lastResponseTime).Nanoseconds() < cleanupPeriod.Nanoseconds()/2 {
				continue
			}
		}else{
			if len(n.pendingQueries)>maxNodePendingQueries{
				log.V(4).Infof("DHT: Node never replied to ping. Deleting. %v", n.address)
				r.kill(n, p)
				continue
			}
		}
	PING:
		needPing = append(needPing,n)
	}
	duration := time.Since(t0)
	log.V(3).Info("DHT: Routing table cleanup took %v", duration)
	return needPing
}

// 如果节点n比最近邻居8个节点更近，neighborhoodUpkeep会通过替换最不近的节点（boundar），n.id 来更新路由表。
func (r *routingTable) neighborhoodUpkeep(n *remoteNode,proto string,p *peerStore){
	if r.boundaryNode == nil {
		r.addNewNeighbor(n,false,proto,p)
		return
	}
	if r.length()<kNodes{
		r.addNewNeighbor(n,false,proto,p)
		return
	}
	cmp := commonBits(r.nodeId,n.id)
	if cmp == 0 {
		return
	}
	if cmp > r.proximity{
		r.addNewNeighbor(n,true,proto,p)
		return
	}
}

func (r *routingTable) addNewNeighbor(n *remoteNode, displaceBoundary bool, proto string, p *peerStore) {
	if err := r.insert(n,proto);err != nil{
		log.V(3).Infof("addNewNeighbor error: %v", err)
		return
	}
	if displaceBoundary && r.boundaryNode != nil {
		r.kill(r.boundaryNode,p)
	}else{
		r.resetNeighborhoodBoundary()
	}
	log.V(4).Infof("New neighbor added %s with proximity %d", nettools.BinaryToDottedPort(n.addressBinaryFormat), r.proximity)
}

// pingSlowly  ping到需要ping的远程节点，在整个cleanupPeriod期间分发ping信号，避免网络流量的爆发。
// 其并没有真正发送ping，而是向主goroutine发出信号，在协程中会ping节点，使用pingRequest通道。
func pingSlowly(pingRequest chan *remoteNode,needPing []*remoteNode,cleanupPeriod time.Duration,stop chan bool) {
	if len(needPing) == 0{
		return
	}
	duration := cleanupPeriod - (1*time.Minute)
	perPingWait := duration / time.Duration(len(needPing))
	for _,r := range needPing{
		pingRequest <- r
		select {
		case <-time.After(perPingWait):
		case <-stop:
			return
		}
	}
}

/*
	expvar包，通过JSON格式的HTTP API公开应用程序和GO运行时的指标
	NewMap会创建一个新的map,并使用expvar.Publish 注册它，然后返回一个指向新创建的map的指针。
 */
var (
	// totalkillednode是一个单调递增的计数器，它是在路由表中被杀死的。如果一个节点稍后被添加到路由表并再次被杀死，那么它将被计算两次。
	totalKilledNodes = expvar.NewInt("totalKilledNodes")
	// totalnode是一个单调递增的计数器，它被添加到路由表中。如果一个节点被删除，然后再添加，它将被计算两次。
	totalNodes = expvar.NewInt("totalNodes")
	// reachableNodes是来自特定DHT节点的所有可到达节点的计数。map键是本地节点的infohash。
	// 该值是一个带有可到达节点数的指标，在最近一次路由表被持久化到磁盘上。
	reachableNodes = expvar.NewMap("reachableNodes")
)















