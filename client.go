package gozkclient

import (
	logger "github.com/kordar/gologger"
	"github.com/samuel/go-zookeeper/zk"
	"path"
	"strings"
	"time"
)

import (
	"sync"
)

type ZkClient struct {
	conn       *zk.Conn
	events     chan zk.Event
	exists     map[string]bool
	listenOnce sync.Once
	isChild    bool
}

func NewZkClient(hosts []string, timeout time.Duration) *ZkClient {
	return NewZkClientWithCallback(hosts, timeout, nil)
}

func NewZkClientWithCallback(hosts []string, timeout time.Duration, cb zk.EventCallback) *ZkClient {
	client := &ZkClient{
		exists: make(map[string]bool),
		events: make(chan zk.Event),
	}

	var err error
	if cb != nil {
		eventCallbackOption := zk.WithEventCallback(func(event zk.Event) {
			cb(event)
			client.events <- event
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

func (c *ZkClient) Add(path string, data []byte, flag int32) (string, error) {
	acl := zk.WorldACL(zk.PermAll)
	return c.AddWithAcl(path, data, flag, acl...)
}

func (c *ZkClient) AddAll(path string, data []byte, flag int32) (string, error) {
	acl := zk.WorldACL(zk.PermAll)
	return c.AddAllWithAcl(path, data, flag, acl...)
}

func (c *ZkClient) AddWithAcl(path string, data []byte, flag int32, acl ...zk.ACL) (string, error) {

	// flags有4种取值：
	// 0:永久，除非手动删除
	// zk.FlagEphemeral = 1:短暂，session断开则该节点也被删除
	// zk.FlagSequence  = 2:会自动在节点后面添加序号
	// 3:Ephemeral和Sequence，即，短暂且自动添加序号

	// 获取访问控制权限
	//acls := zk.WorldACL(zk.PermAll)
	return c.conn.Create(path, data, flag, acl)
}

func (c *ZkClient) AddAllWithAcl(paths string, data []byte, flag int32, acl ...zk.ACL) (string, error) {
	ss := strings.Split(paths, "/")
	dir := path.Base("/")
	for _, s := range ss {
		dir = path.Join(dir, s)
		if exists, _, _ := c.Exists(dir); !exists {
			if dir == paths {
				break
			}
			if parent, err2 := c.AddWithAcl(dir, []byte(""), flag, acl...); err2 == nil {
				logger.Infof("创建父目录%s完成", parent)
			}
		}
	}

	return c.AddWithAcl(dir, data, flag, acl...)
}

func (c *ZkClient) Exists(path string) (bool, *zk.Stat, error) {
	return c.conn.Exists(path)
}

func (c *ZkClient) Get(path string) []byte {
	data, _, err := c.conn.Get(path)
	if err != nil {
		logger.Warnf("查询%s失败, err: %v", path, err)
		return nil
	}
	return data
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
	logger.Infof("删除目录%s完成", path)
	return true
}

func (c *ZkClient) DelAll(paths string) bool {
	for len(paths) > 1 {
		if exists, _, _ := c.Exists(paths); exists {
			c.Del(paths)
		}
		paths = path.Dir(paths)
	}
	if paths == "/" {
		return true
	}
	return c.Del(paths)
}

func (c *ZkClient) Children(path string) ([]string, *zk.Stat, error) {
	return c.conn.Children(path)
}

func (c *ZkClient) Listen(path ...string) {
	c.listenOnce.Do(c.startListen)
	// 开始监听path
	if c.isChild {
		logger.Warn("已监听children，禁止exist监听")
		return
	}
	for _, s := range path {
		_, _, _, err := c.conn.ExistsW(s)
		if err != nil {
			logger.Warnf("监听path=%s, err=%v", s, err)
			continue
		}
		c.exists[s] = true
	}
}

func (c *ZkClient) ListenChildren(path ...string) {
	c.listenOnce.Do(func() {
		c.startListen()
		c.isChild = true
	})
	if !c.isChild {
		logger.Warn("已监听exist，禁止children监听")
		return
	}
	// 开始监听path
	for _, s := range path {
		_, _, _, err := c.conn.ChildrenW(s)
		if err != nil {
			logger.Warnf("监听path%s子目录, err=%v", s, err)
			continue
		}
		c.exists[s] = true
	}
}

func (c *ZkClient) startListen() {
	go func() {
		for {
			select {
			// 有消息则取出队列的Request，并执行绑定的业务方法
			case event := <-c.events:
				p := event.Path
				if p == "" {
					break
				}
				if c.isChild {
					c.ListenChildren(p)
				} else {
					c.Listen(p)
				}
			}
		}
	}()
}

func (c *ZkClient) Close() {
	c.conn.Close()
}
