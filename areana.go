package dht

type arena chan []byte

/*
	arena是一个免费的列表，它提供了对预先分配的byte slices的快速访问，极大地减少了内存的流失，并有效地禁用了这些分配的GC。
	在arena创建之后，可以通过调用Pop（）来请求一个bytes的切片slice。
	调用者负责调用Push（），后者将block放回队列中以便以后使用。Pop（）给出的字节不是0，
	所以调用者应该只读取它知道已经被过度使用的位置。这可以通过缩短正确位置的切片来完成，
	这是基于Write（）和类似函数返回的字节数。
 */

func newArena(blockSize int,numBlocks int) arena {
	blocks := make(arena,numBlocks)
	for i := 0;i <numBlocks;i++ {
		blocks <- make([]byte,blockSize)
	}
	return blocks
}
func (a arena) Pop() (x []byte) {
	return <-a
}
func (a arena) Push(x []byte) {
	x = x[:cap(x)]
	a <- x
}
