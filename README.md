# medis
mysql cluster work as a redis node

## 架构图
![架构图](https://raw.githubusercontent.com/wudikua/medis/master/medis.png)

* group层是虚拟的，是一组读写分离的mysql数据源
* key会hash到某一个gourp在这个group上做读写
* 扩容期间，会自动在多个group上做双写
* 按照目前的架构，写瓶颈通过扩容group解决，读瓶颈通过扩容group中用于读的mysql从库解决

## 启动

* go run medis.go

### 启动代码中的group的配置需要说明一下

```
// 创建主从关系
group0 := datasource.NewGroup("group0")
// 0 1 0 1 代表的是 读权重是0，写权重是1，不可读，写优先级是1
group0.AddClient(datasource.NewClientWeightWrapper("group0_master_0", master0, 0, 1, 0, 1))
group0.AddClient(datasource.NewClientWeightWrapper("group0_slave_0", master0, 1, 0, 1, 0))
group0.Init()
group1 := datasource.NewGroup("group1")
group1.AddClient(datasource.NewClientWeightWrapper("group1_master_0", master1, 0, 1, 0, 1))
group1.AddClient(datasource.NewClientWeightWrapper("group1_slave_0", master1, 1, 0, 1, 0))
group1.Init()
```

* 首先一个group中是按照优先级优先选择mysql，高优先级还可用时才用低优先级
* 所以低优先级的mysql可以理解为备机
* 其次读写权重的意义是，按照权重的来选择用哪个mysql，一般写只有一个，所以写权重意义目前不大