package goframework_zk

import (
	logger "github.com/kordar/gologger"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZkClient struct {
	conn  *zk.Conn
	wPath []string
}

func NewZkClient(hosts []string, timeout time.Duration) *ZkClient {
	return NewZkClientWithCallback(hosts, timeout, nil)
}

func NewZkClientWithCallback(hosts []string, timeout time.Duration, cb zk.EventCallback) *ZkClient {
	client := &ZkClient{wPath: make([]string, 0)}

	var err error
	if cb != nil {
		eventCallbackOption := zk.WithEventCallback(func(event zk.Event) {
			cb(event)
			for _, p := range client.wPath {
				client.Listen(p)
			}
		})
		client.conn, _, err = zk.Connect(hosts, timeout, eventCallbackOption)
	} else {
		client.conn, _, err = zk.Connect(hosts, timeout)
	}

	if err != nil {
		logger.Errorf("初始化zookeeper失败，err=%v", err)
		return nil
	}

	return client
}

func (c *ZkClient) Add(path string, data []byte, flag int32) bool {
	acl := zk.WorldACL(zk.PermAll)
	return c.AddWithAcl(path, data, flag, acl...)
}

func (c *ZkClient) AddWithAcl(path string, data []byte, flag int32, acl ...zk.ACL) bool {

	// flags有4种取值：
	// 0:永久，除非手动删除
	// zk.FlagEphemeral = 1:短暂，session断开则该节点也被删除
	// zk.FlagSequence  = 2:会自动在节点后面添加序号
	// 3:Ephemeral和Sequence，即，短暂且自动添加序号

	// 获取访问控制权限
	//acls := zk.WorldACL(zk.PermAll)
	s, err := c.conn.Create(path, data, flag, acl)
	if err != nil {
		logger.Warnf("创建失败: %v", err)
		return false
	}
	logger.Infof("创建: %s 成功", s)
	return true
}

func (c *ZkClient) Get(path string) bool {
	data, _, err := c.conn.Get(path)
	if err != nil {
		logger.Warnf("查询%s失败, err: %v", path, err)
		return false
	}
	logger.Infof("%s 的值为 %s", path, string(data))
	return true
}

// Modify 删改与增不同在于其函数中的version参数,其中version是用于 CAS支持
// 可以通过此种方式保证原子性
// 改
func (c *ZkClient) Modify(path string, data []byte) bool {
	_, sate, _ := c.conn.Get(path)
	_, err := c.conn.Set(path, data, sate.Version)
	if err != nil {
		logger.Warnf("数据修改失败: %v", err)
		return false
	}
	return true
}

func (c *ZkClient) Del(path string) bool {
	_, sate, _ := c.conn.Get(path)
	err := c.conn.Delete(path, sate.Version)
	if err != nil {
		logger.Warnf("数据删除失败: %v", err)
		return false
	}
	return true
}

func (c *ZkClient) Listen(path ...string) {
	// 开始监听path
	for _, s := range path {
		_, _, _, err := c.conn.ExistsW(s)
		if err != nil {
			logger.Warnf("监听path=%s, err=%v", s, err)
			continue
		}
		exists := false
		for _, p := range c.wPath {
			if p == s {
				exists = true
				break
			}
		}
		if !exists {
			c.wPath = append(c.wPath, s)
		}
	}

}

func (c *ZkClient) Close() {
	c.conn.Close()
}
