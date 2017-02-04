#sqlite_queue_python

基于python实现的sqlite队列，方便的处理sqlite并发。

讲道理，写这个库的人并不会写python。SqliteQueue是继承了threading.Thread的线程，并且维护了一个向sqlite请求的队列。

支持但是不必须peewee

## Example

一个简单的队列可以在两行代码实现。

```python
queue = sqlite_queue.SqliteQueue('test.db')
queue.start()
```

以下是一个完整的示例：

```python
queue = sqlite_queue.SqliteQueue('test.db')
queue.setDaemon(False)  # 默认为守护线程
queue.start()

# 测试INSERT
for i in range(12,15):
    queue.register_execute("INSERT INTO stocks VALUES ('2017-02-04','BUY','RHAT',?,35.14)", (i,)
		, callback=lambda lst_row, data: print(lst_row))

# 测试SELECT
queue.register_execute("SELECT * FROM stocks", callback=lambda lst_row, data: print(data))
```

## TODO List

- 实现简单的SQL语句拼接
- 实现WHERE语句拼接
- 实现对回调函数参数的自动识别、匹配
