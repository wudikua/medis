package datasource

type Weight struct {
	// 读权重 读优先级
	r, p int
	// 写权重 写优先级
	w, q int
}
