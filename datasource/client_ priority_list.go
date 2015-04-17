package datasource

import ()

// 一组不同权重的读或者写的数据源
type ClientPriorityList struct {
	clients       []*ClientWeightWrapper
	lowerPriority *ClientPriorityList
	priority      int
	i             int
	cw            int
	gcd           int
	num           int
	r_max         int
	w_max         int
}

func NewClientPriorityList(clients []*ClientWeightWrapper, priority int) *ClientPriorityList {
	list := &ClientPriorityList{
		clients:  clients,
		priority: priority,
		i:        0,
		cw:       0,
		gcd:      1,
		num:      len(clients),
		r_max:    -1,
		w_max:    -1,
	}
	for _, c := range clients {
		if c.w > list.w_max {
			list.w_max = c.w
		}
		if c.r > list.r_max {
			list.r_max = c.r
		}
	}
	return list
}

func (self *ClientPriorityList) SelectRead() *ClientWeightWrapper {
	if self.num == 1 {
		return self.clients[0]
	}
	for {
		self.i = (self.i + 1) % self.num
		if self.i == 0 {
			self.cw = self.cw - self.gcd
			if self.cw <= 0 {
				self.cw = self.r_max
				if self.cw == 0 {
					return nil
				}
			}
		}

		if weight := self.clients[self.i].r; weight >= self.cw {
			return self.clients[self.i]
		}
	}
	if self.lowerPriority != nil {
		return self.lowerPriority.SelectRead()
	}
	return nil
}

func (self *ClientPriorityList) SelectWrite() *ClientWeightWrapper {
	if self.num == 1 {
		return self.clients[0]
	}
	for {
		self.i = (self.i + 1) % self.num
		if self.i == 0 {
			self.cw -= self.gcd
			if self.cw <= 0 {
				self.cw = self.w_max
				if self.cw == 0 {
					return nil
				}
			}
		}

		if weight := self.clients[self.i].w; weight >= self.cw {
			return self.clients[self.i]
		}
	}
	if self.lowerPriority != nil {
		return self.lowerPriority.SelectWrite()
	}
	return nil
}
