package dht

import (
	"container/ring"
	"github.com/golang/groupcache/lru"
	log "github.com/golang/glog"
)

// For the inner map,key地址是二进制格式，value=ignored
type peerContactsSet struct {
	set map[string]bool
	// 需要确保不同的peers在不同时间返回
	ring *ring.Ring
}

// next() 返回8个节点联系，如果可能，将来调用会返回一个不同的联系集合
func (p *peerContactsSet) next() []string {
	count := kNodes
	if count > len(p.set){
		count = len(p.set)
	}
	x := make([]string,0,count)
	xx := make(map[string]bool)
	for range p.set {
		nid := p.ring.Move(1).Value.(string)
		if _,ok := xx[nid];p.set[nid] && !ok {
			xx[nid] = true
		}
		if len(xx) >= count{
			break
		}
	}
	if len(xx) < count {
		for range p.set{
			nid := p.ring.Move(1).Value.(string)
			if _,ok := xx[nid];ok {
				continue
			}
			if len(xx) >= count {
				break
			}
		}
	}
	for id := range xx {
		x = append(x,id)
	}
	return x
}

// 将一个peerContact添加到infohash联系人集。 peerContact必须是一个二进制编码的联系人地址，其中前四个字节形成IP，最后一个字节是端口。
// 目前还不支持IPv6地址。不能存储少于6字节的peerContact。
func (p *peerContactsSet) put(peerContact string) bool {
	if len(peerContact) < 6 {
		return  false
	}
	if ok := p.set[peerContact];ok {
		return false
	}
	p.set[peerContact] = true  			// peerContact是字符串，将peerContactsSet的set集合中Key为peerContact设为true
	r := &ring.Ring{Value:peerContact}  // peerContact构建一个新的环形链表r
	if p.ring == nil {					// 如果peerContactsSet的ring为空，设置为r
		p.ring = r
	}else{
		p.ring.Link(r)					// 如果peerContactsSet的ring非空，ring和r不是同一个环形链表，r添加到ring后面
	}
	return true
}

// 如果在参数为空的情况下，它会先尝试删除一个死的对等点，然后删除联系人，并删除联系人。
func (p *peerContactsSet) drop(peerContact string) string {
	if peerContact == "" {
		if c := p.dropDead();c != "" {						// 如果有死元素
			return c										// 返回这个死元素
		}else {												// 如果没有死元素
			return p.drop(p.ring.Next().Value.(string))	// 然后从环形链表上拿下一个元素进行删除
		}
	}
	for i := 0; i < p.ring.Len()+1; i++ {						// 经过上面这样递归删除死元素的操作后，对环形链表进行遍历
		if p.ring.Move(1).Value.(string) == peerContact {	// 如果环形链表移动一个位置的元素跟peerContact相等
			dn := p.ring.Unlink(1).Value.(string)			// 从环形链表上删除这个元素
			delete(p.set, dn)									// 从peerContactsSet的set中删除这个元素
			return dn											// 返回这个元素
		}
	}
	return ""
}
func (p *peerContactsSet) dropDead() string {
	for i := 0;i < p.ring.Len()+1; i++ {				// 对环形链表进行遍历
		if !p.set[p.ring.Move(1).Value.(string)] {  // 如果peerContactsSet的set中，Key为某个infohash的Value为false
			dn := p.ring.Unlink(1).Value.(string)   // 从环形链表上删除这个元素
			delete(p.set, dn)							// 从peerContactsSet的set中删除这个元素
			return  dn									// 返回这个死元素
		}
	}
	return ""
}

func (p *peerContactsSet) kill(peerContact string) {
	if ok := p.set[peerContact]; ok {					// 如果在peerContactsSet中找到了peerContact
		p.set[peerContact] = false						// 将其Value修改成false，在后面进行drop的时候会将其清理出去
	}
}

// Size() 对一个infohash，已知的联系人数量
func (p *peerContactsSet) Size() int {
	return len(p.set)
}

func (p *peerContactsSet) Alive() int {
	var ret int = 0
	for ih := range p.set {
		if p.set[ih] {
			ret++
		}
	}
	return ret
}

type peerStore struct {
	//为infohash缓存对等点。每一个键都是一个infohash，而值是peerContactsSet。
	infoHashPeers *lru.Cache
	localActiveDownloads map[InfoHash]bool
	maxInfoHashes int
	maxInfoHashPeers int
}

func newPeerStore(maxInfoHashes,maxInfoHashPeers int) *peerStore {
	return &peerStore{
		infoHashPeers:lru.New(maxInfoHashes),
		localActiveDownloads:make(map[InfoHash]bool),
		maxInfoHashes:maxInfoHashes,
		maxInfoHashPeers:maxInfoHashPeers,
	}
}

// 从peerStore中，从缓存中把Key为infohash的Value查找出来
func (h *peerStore) get(ih InfoHash) *peerContactsSet {
	c, ok := h.infoHashPeers.Get(string(ih)) 	// 从缓存中查找
	if !ok {
		return nil
	}
	contacts := c.(*peerContactsSet) 			// 断言，判断c是否为*peerContactsSet  指针类型
	return contacts
}

// count shows the number of known peers for the given infohash.
func (h *peerStore) count(ih InfoHash) int {
	peers := h.get(ih)
	if peers == nil {
		return 0
	}
	return peers.Size()
}

func (h *peerStore) alive(ih InfoHash) int {
	peers := h.get(ih)
	if peers == nil {
		return 0
	}
	return peers.Alive()
}

// peerContacts returns a random set of 8 peers for the ih InfoHash.
func (h *peerStore) peerContacts(ih InfoHash) []string {
	peers := h.get(ih)
	if peers == nil {
		return nil
	}
	return peers.next()
}

// addContact() 作为一个提供infohash的对等点，如果联系人已经添加了，返回true。否则false（例如已经存在或无效了）。
func (h *peerStore) addContact(ih InfoHash, peerContact string) bool {
	var peers *peerContactsSet
	p, ok := h.infoHashPeers.Get(string(ih))  // 从LRU缓存中获取infohash的peer节点集合
	if ok {
		var okType bool
		peers, okType = p.(*peerContactsSet)				// 断言
		if okType && peers != nil {
			if peers.Size() >= h.maxInfoHashPeers {    		// 如果缓存的peer节点数大于最大的保存数目
				if _, ok := peers.set[peerContact]; ok { 	// 判断peerContact是否已经在peerContactsSet中，如果在，返回false
					return false
				}
				if peers.drop("") == "" {		// 如果没有死元素，返回false
					return false
				}
			}
			h.infoHashPeers.Add(string(ih), peers)			// 将 Key=infohash,Value=peerContactsSet的map保存到peerStore中
			return peers.put(peerContact)					// 将peerContact保存到peerContactsSet中
		}
		// Bogus peer contacts, reset them.
	}
	peers = &peerContactsSet{set: make(map[string]bool)}
	h.infoHashPeers.Add(string(ih), peers)
	return peers.put(peerContact)
}

func (h *peerStore) killContact(peerContact string) {
	if h == nil {
		return
	}
	for ih := range h.localActiveDownloads {
		if p := h.get(ih); p != nil {
			p.kill(peerContact)
		}
	}
}

func (h *peerStore) addLocalDownload(ih InfoHash) {
	h.localActiveDownloads[ih] = true
}

func (h *peerStore) hasLocalDownload(ih InfoHash) bool {
	_, ok := h.localActiveDownloads[ih]
	log.V(3).Infof("hasLocalDownload for %x: %v", ih, ok)
	return ok
}