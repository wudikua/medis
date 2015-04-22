package datasource

import (
	"fmt"
)

type Weight struct {
	// 读权重 读优先级
	r int `json:"r"`
	p int `json:"p"`
	// 写权重 写优先级
	w int `json:"w"`
	q int `json:"q"`
}

func (w Weight) String() string {
	return fmt.Sprintf("r:%d p:%d w:%d q:%d \n", w.r, w.p, w.w, w.q)
}
