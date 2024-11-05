## 思路分析

### master
- 设计数据结构初始化每个task的input
- 分发task给worker(当woker通过rpc请求) (上锁)
- 接收woker传递的task信息(rpc通信)

### rpc
- worker与master的通信

### worker
- 设计map&reduce处理逻辑
- 向master请求task
- 处理map||reduce task


### detail
- Map task:获取task文件内容->mapf处理得到中间键值对->分成r份kvs->使用临时文件存储中间键值对->文件全部写完自动重命名（最后两步防止程序崩溃文件内容不完整）
