# sqlite_queue_python

[Origin repo](https://gitee.com/kaaass/sqlite_queue_python)

A simple sqlite queue with a SQL statement encapsulation. A convenient solution of sqlite concurrency.

SqliteQueue maintains a queue of sqlite query. Support peewee request. SqlQuery contains a simple SQL statement encapsulation.

## Example

A simple queue could be created in just 2 lines.

```python
import sqlite_queue

queue = sqlite_queue.SqliteQueue('test.db')
queue.start()
```

Here is a complete example.

```python
import sqlite_queue

queue = sqlite_queue.SqliteQueue('test.db')
queue.setDaemon(False)  # Default a daemon thread
queue.start()

# INSERT statement
for i in range(12,15):
    queue.register_execute("INSERT INTO stocks VALUES ('2017-02-04','BUY','RHAT',?,35.14)", (i,)
		, callback=lambda lst_row, data: print(lst_row))

# SELECT statement
queue.register_execute("SELECT * FROM stocks", callback=lambda lst_row, data: print(data))

# Encapsulation of SELECT statement. 
queue.select('stocks').where('price', '>=', 30) \
                        .order('price').page(1, 5)\
                        .register(callback=lambda lst_row, data: print(data))
```

## Installation

For installation, just run:

```python
python setup.py install
```

## TODO List

- 完成：_实现简单的SQL语句拼接_
- 完成：_实现WHERE语句拼接_
- 完成：_实现对回调函数参数的自动识别、匹配_
