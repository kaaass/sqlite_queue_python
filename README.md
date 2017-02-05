#sqlite_queue_python

基于python实现的sqlite队列，方便的处理sqlite并发。并且包含一个十分简洁好用的SQL语句包装。

讲道理，写这个库的人并不会写python。

SqliteQueue是继承了threading.Thread的线程，并且维护了一个向sqlite请求的队列。支持peewee请求。SqlQuery简单的封装了SQL语句。

## Example

一个简单的队列可以在两行代码实现。

```python
import sqlite_queue

queue = sqlite_queue.SqliteQueue('test.db')
queue.start()
```

以下是一个完整的示例：

```python
import sqlite_queue

queue = sqlite_queue.SqliteQueue('test.db')
queue.setDaemon(False)  # 默认为守护线程
queue.start()

# 测试INSERT
for i in range(12,15):
    queue.register_execute("INSERT INTO stocks VALUES ('2017-02-04','BUY','RHAT',?,35.14)", (i,)
		, callback=lambda lst_row, data: print(lst_row))

# 测试SELECT
queue.register_execute("SELECT * FROM stocks", callback=lambda lst_row, data: print(data))

# 包装SQL语句SELECT
queue.select('stocks').where('price', '>=', 30) \
                        .order('price').page(1, 5)\
                        .register(callback=lambda lst_row, data: print(data))
```

## TODO List

- 完成：_实现简单的SQL语句拼接_
- 完成：_实现WHERE语句拼接_
- 实现对回调函数参数的自动识别、匹配
