package dht

import log "github.com/golang/glog"

/**
	DHT 路由使用一个二叉树，没有桶
	节点的ID是20个字节，当从自己活着远程主机扫描一个infohash时，节点需要从自己的路由表和最近的节点上查看并返回。
	所为最近的节点，这个距离是如何衡量呢？
	节点node和infohash之间使用XOR距离，各自的字符串计算XOR距离。意味着只能以infohash为支点，将所有距离远近的节点排序。
	很多bittorrent/Kademlia DHT 实现都使用了与桶的使用bit-by-bit相比较的混合。
	但我想尝试一些不同的东西，不使用桶。桶有一个单独的id，一个根据它计算距离，加速查找。
	我决定把路由表放在一个二叉树中，这更直观。目前，实现是一棵真正的树，而不是一个自由列表，但它运行良好。
	所有节点都插入到二叉树中，固定高度为160（20字节）。为了查找infohash，我使用每个级别的infohash位来进行inorder遍历。
	在大多数情况下，查找到达树的底部，而不会触及目标信息散列，因为在绝大多数情况下，它不在我的路由表中。
	然后，我只是继续按顺序遍历（然后是“左”），然后在收集8个最近的节点之后返回。
	为了加快速度，我尽量让树尽量短。如果在插入另一个节点时发生冲突，那么通往每个节点的路径将被压缩，并在稍后进行未压缩。
	我不知道与使用桶的实现相比，整个算法有多慢，但是对于值得的，路由表查找甚至不再显示在CPU配置文件上了。
 */

 type nTree struct {
 	zero, one  *nTree
 	value *remoteNode
 }

 const (
 	kNodes = 8						// 每次请求都返回kNodes个节点
 	maxNodePendingQueries = 5 	// 如果一个节点访问数量超过maxNodePendingQueries，认为其是旧的
 )

 // 递归版本的节点插入
 func (n *nTree) insert(newNode *remoteNode){
 	n.put(newNode,0)
 }
 // 关键是理解参数i，node的id是20个字节，8进制，160位
func (n *nTree) put(newNode *remoteNode, i int) {
	if i >= len(newNode.id)*8 { // 如果要插入的位置i（二叉树上的位置）大于或等于新节点id的位bit，直接就更新nTree的value
		n.value = newNode
		return
	}
	if n.value != nil {
		// 如果新节点的id跟树存储的节点id相等，直接替换
		if n.value.id == newNode.id {
			n.value = newNode
			return
		}
		// 如果不相等，就需要有分支出来存储新的节点
		old := n.value
		n.value = nil
		n.branchOut(newNode,old,i)
		return
	}
	chr := byte(newNode.id[i/8])
	bit := byte(i%8)
	if (chr<<bit)&128 != 0 {
		if n.one == nil {
			n.one = &nTree{value:newNode}
			return
		}
		n.one.put(newNode,i+1)
	}else{
		if n.zero == nil {
			n.zero = &nTree{value:newNode}
			return
		}
		n.zero.put(newNode,i+1)
	}
}
func (n *nTree) branchOut(n1 *remoteNode, n2 *remoteNode, i int) {
	// 因为它们是分支的，所以它保证在这个分支下面没有其他节点，所以只要创建各自的节点，直到它们各自的位不同。
	chr := byte(n1.id[i/8])			// 根据在树上的插入位置i，算出新节点id 0-16bits处在哪一个bit位置
	bitPos := byte(i%8)				// 根据在树上的插入位置i，算出需要移动的bit
	bit := (chr << bitPos) & 128	// 与128进行与运算，大于等于128就等于128，小于128就等于0

	chr2 := byte(n2.id[i/8])
	bitPos2 := byte(i%8)
	bit2 := (chr2 << bitPos2) & 128

	if bit != bit2 { 				// 如果新旧节点的位置相同，将新旧节点插入这棵树
		n.put(n1,i)
		n.put(n2,i)
		return
	}
	if bit != 0 {					// 如果新旧节点的位置相同且新节点算出的位置不等于0，将这棵树的one进行分支扩展
		n.one = &nTree{}
		n.one.branchOut(n1,n2,i+1)
	}else{
		n.zero = &nTree{}			// 如果新旧节点的位置相同且新节点算出的位置等于0，将这棵树的zero进行分支扩展
		n.zero.branchOut(n1,n2,i+1)
	}
}

func (n *nTree) lookup(id InfoHash) []*remoteNode {
	ret := make([]*remoteNode,0,kNodes)
	if n == nil || id == "" {
		return nil
	}
	return n.traverse(id,0,ret,false)
}
func (n *nTree) traverse(id InfoHash, i int, ret []*remoteNode, filter bool) []*remoteNode {
	if n == nil {
		return ret
	}
	if n.value != nil {
		if !filter || n.isOK(id) {
			return append(ret,n.value)
		}
	}
	if i >= len(id)*8 {
		return ret
	}
	if len(ret) >= kNodes{
		return ret
	}
	chr := byte(id[i/8])
	bit := byte(i % 8)
	var left, right *nTree
	if (chr<<bit)&128 != 0 {
		left = n.one
		right = n.zero
	}else{
		left = n.zero
		right = n.one
	}
	ret = left.traverse(id,i+1,ret,filter)
	if len(ret) >= kNodes{
		return ret
	}
	return right.traverse(id,i+1,ret,filter)
}

func (n *nTree) isOK(ih InfoHash) bool{
	if n.value == nil || n.value.id == "" {
		return false
	}
	r := n.value
	if len(r.pendingQueries)>maxNodePendingQueries{
		log.V(3).Infof("DHT: Skipping because there are too many queries pending for this dude.")
		log.V(3).Infof("DHT: This shouldn't happen because we should have stopped trying already. Might be a BUG.")
		return false
	}
	recent := r.wasContactedRecently(ih)
	if log.V(4) {
		log.Infof("wasContactedRecently for ih=%x in node %x@%v returned %v", ih, r.id, r.address, recent)
	}
	if recent {
		return false
	}
	return true
}

// 如果所有的叶子都是空的，就会把树砍下来，然后删除子节点。
func (n *nTree) cut(id InfoHash,i int) (cutMe bool) {
	if n == nil {
		return true
	}
	if i >= len(id)*8 {
		return true
	}
	chr := byte(id[i/8])
	bit := byte(i % 8)
	if (chr<<bit)&128 != 0{
		if n.one.cut(id,i+1){
			n.one = nil
			if n.zero == nil {
				return true
			}
		}
	}else{
		if n.zero.cut(id,i+1){
			n.zero = nil
			if n.one == nil {
				return true
			}
		}
	}
	return false
}

func commonBits(s1, s2 string) int {
	// copied from jch's dht.cc.
	id1, id2 := []byte(s1), []byte(s2)

	i := 0
	for ; i < 20; i++ {
		if id1[i] != id2[i] {
			break
		}
	}

	if i == 20 {
		return 160
	}

	xor := id1[i] ^ id2[i]

	j := 0
	for (xor & 0x80) == 0 {
		xor <<= 1
		j++
	}
	return 8*i + j
}

// 计算两个哈希之间的距离。在dht/kademlia中，“距离”是torrent infohash和对等节点ID的XOR，这比必要的要慢。应该只用于显示友好的消息。
func hashDistance(id1 InfoHash, id2 InfoHash) (distance string) {
	d := make([]byte, len(id1))
	if len(id1) != len(id2) {
		return ""
	} else {
		for i := 0; i < len(id1); i++ {
			d[i] = id1[i] ^ id2[i]
		}
		return string(d)
	}
	return ""
}

func (n *nTree) lookupFiltered(ih InfoHash) []*remoteNode{
	ret := make([]*remoteNode,0,kNodes)
	if n==nil||ih==""{
		return nil
	}
	return n.traverse(ih,0,ret,true)
}
