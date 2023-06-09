## bitcask

bitcask的实例就是系统上的一个目录，限制同一时刻只能一个进程打开这个目录。目录中有多个文件，但是同一时刻只有一个active data file可以被写入新的数据。当一个active data file被写满后，就会关闭该文件并成为older data file，并打开新的一个文件进行写入。

每一次写入都是追加写入，删除操作实际上也是一次追加写入，只不过写入的是一个特殊的tombstone value，用于标记一次记录的删除，并不会实际地去原地删除该记录。当下一次merge的时候才会将这种无效数据进行清理。

keydir 是一个简单的hash table，key- >fixed-size structure 



## 实践

### 内存索引

首先内存索引的数据结构bitcast里是hash table，使用哈希表可以快速读取，但是无法遍历数据。如果希望数据有序，我们也可以选择b tree或者跳表。