package datasource

import (
	"fmt"
	"medis/mysql"
)

// 带权重优先级的client
type ClientWeightWrapper struct {
	name   string
	client *mysql.MysqlClient
	Weight
}

// 配置规则 r w p q的单位都是1 小于1 代表不可用
func NewClientWeightWrapper(name string, client *mysql.MysqlClient, r int, w int, p int, q int) *ClientWeightWrapper {
	wrapper := &ClientWeightWrapper{
		client: client,
		name:   name,
	}
	wrapper.r = r
	wrapper.w = w
	wrapper.p = p
	wrapper.q = q
	return wrapper
}

func (self *ClientWeightWrapper) String() string {
	return fmt.Sprintf("name:%s \n client:%s \n weight:%s \n", self.name, self.client, self.Weight)
}

func (self *ClientWeightWrapper) GetClient() *mysql.MysqlClient {
	return self.client
}
