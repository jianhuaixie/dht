package main

import "time"
import (
	"crypto/rand"
	"fmt"
	"encoding/hex"
)

func newTransactionId() int {
	n, err := rand.Read(make([]byte, 1))
	if err != nil {
		return time.Now().Second()
	}
	return n
}

type InfoHash string

func DecodeInfoHash(x string) (b InfoHash,err error){
	var h []byte
	h,err = hex.DecodeString(x)
	if len(h)!=20{
		return "", fmt.Errorf("DecodeInfoHash: expected InfoHash len=20, got %d", len(h))
	}
	return InfoHash(h),err
}

func TestDocodeInfoHash(){
	infoHash,err := DecodeInfoHash("d1c5676ae7ac98e8b19f63565905105e3c4c37a2")
	if err != nil{
		fmt.Errorf("DecodeInfoHash faiure")
	}
	fmt.Println(infoHash)
	fmt.Println(string(infoHash))
}


func main() {
	n := newTransactionId()
	fmt.Println(n)
	TestDocodeInfoHash()
}
