package dht

import (
	"os"
	"strings"
	"path"
	"github.com/youtube/vitess/go/vt/log"
	"fmt"
	"encoding/json"
	"io/ioutil"
)

// dhtStore 用来持久化保存路由表到磁盘上
type dhtStore struct {
	Id	[]byte
	Port	int
	Remotes	map[string][]byte	// Key:IP,Value:node ID
	path	string 				// Empty if the store is disabled
}

func mkdirStore() string {
	dir := "var/run/cantontorrent"
	env := os.Environ()
	for _,e := range env {
		if strings.HasPrefix(e,"HOME=") {
			dir = strings.SplitN(e,"=",2)[1]
			dir = path.Join(dir,".cantontorrent")
		}
	}
	os.MkdirAll(dir,0750)
	if s,err := os.Stat(dir);err != nil{
		log.Fatal("stat config dir",err)
	}else if !s.IsDir(){
		log.Fatalf("Dir %v expected directory,got %v",dir,s)
	}
	return dir
}

func openStore(port int,enabled bool) (cfg *dhtStore){
	cfg = &dhtStore{Port:port}
	if enabled{
		cfg.path = mkdirStore()
		p := fmt.Sprintf("%v-%v", path.Join(cfg.path, "dht"), port)
		f,err := os.Open(p)
		if err != nil{
			return cfg
		}
		defer f.Close()
		if err = json.NewDecoder(f).Decode(cfg);err != nil {
			println(err)
		}
	}
	return
}

func saveStore(s dhtStore){
	if s.path == "" {
		return
	}
	tmp,err := ioutil.TempFile(s.path,"cantontorrent")
	if err != nil{
		println("saveStore tempfile:",err)
		return
	}
	err = json.NewEncoder(tmp).Encode(s)
	tmp.Close()
	if err != nil{
		println("saveStore json encoding:",err)
	}
	p := fmt.Sprintf("%v-%v", s.path+"/dht", s.Port)
	if err := os.Rename(tmp.Name(),p);err != nil {
		if err := os.Remove(p);err != nil {
			println("saveStore failed to remove the existing config:",err)
			return
		}
		if err := os.Rename(tmp.Name(),p);err != nil{
			println("saveStore failed to rename file after deleting the original config:",err)
			return
		}
	}else{
		println("saved DHT routing table to the filesystem.")
	}
}