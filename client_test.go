package gozkclient_test

import (
	"fmt"
	logger "github.com/kordar/gologger"
	"github.com/kordar/gozkclient"
	"github.com/samuel/go-zookeeper/zk"
	"testing"
	"time"
)

var pp = "/demo/tt"

func TestAdd(t *testing.T) {
	hosts := []string{"43.139.223.7:62181"}
	zkClient := gozkclient.NewZkClientWithCallback(hosts, time.Second*30, func(event zk.Event) {
		logger.Infof(fmt.Sprintf("xxxxxxxxxxxxxxxxx%v", event))
	})

	zkClient.Listen(pp)
	zkClient.ListenChildren("/demo")

	_, e1 := zkClient.Add(pp, []byte("AAAAA"), 0)
	logger.Errorf("====11111111111111%v", e1 == zk.ErrNodeExists)
	time.Sleep(2 * time.Second)
	//_, e2 := zkClient.Add("/demo2", []byte("AAAAA"), 0)
	//logger.Errorf("====11111111111111%v", e2 == zk.ErrBadArguments)

	//time.Sleep(2 * time.Second)
	//path, err := zkClient.Add(pp, []byte("AAAAA"), 1)
	//zkClient.Listen(pp)
	//
	//time.Sleep(2 * time.Second)
	//zkClient.Modify(pp, []byte("BBBB"))
	//
	//time.Sleep(2 * time.Second)
	//zkClient.Del(pp)
	//time.Sleep(2 * time.Second)

	//logger.Infof(fmt.Sprintf("-------------%+v, %+v", path, err))

	//zkClient.Del("/demo/tt")
	data := zkClient.Get("/demo/tt")

	logger.Infof("=========%v", string(data))

	time.Sleep(5 * time.Second)

	//ps, _, _ := zkClient.Children("/demo")
	//logger.Infof("===========%v", ps)
}

func TestZkClient_AddAll(t *testing.T) {
	hosts := []string{"43.139.223.7:62181"}
	zkClient := gozkclient.NewZkClientWithCallback(hosts, time.Second*30, func(event zk.Event) {
		logger.Infof(fmt.Sprintf("xxxxxxxxxxxxxxxxx%v", event))
	})

	zkClient.Listen("/AA/BB/CC/DD")
	_, ee := zkClient.AddAll("/AA/BB/CC/DD", []byte("mmmmm"), 0)
	logger.Errorf("eeeeeeeeeeeeee%v", ee)

	children, _, _ := zkClient.Children("/")
	logger.Infof("ccccccccccccccccc=%v", children)

}

func TestZkClient_DelAll(t *testing.T) {
	hosts := []string{"43.139.223.7:62181"}
	zkClient := gozkclient.NewZkClientWithCallback(hosts, time.Second*30, func(event zk.Event) {
		logger.Infof(fmt.Sprintf("xxxxxxxxxxxxxxxxx%v", event))
	})

	zkClient.Listen("/AA/BB/CC/DD")
	zkClient.DelAll("/AA/BB/CC/DD")

}
