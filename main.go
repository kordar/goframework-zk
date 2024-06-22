package gozkclient

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

func main() {
	// 创建zk连接地址
	hosts := []string{"43.139.223.7:62181"}
	// 连接zk
	conn, _, err := zk.Connect(hosts, time.Second*5)
	defer conn.Close()
	if err != nil {
		fmt.Println("eeeeeee", err)
		return
	}
	println(conn.Server())
}
