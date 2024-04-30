# my2sql 解析MySQL binlog生成回滚前滚sql 增强版本

原版github地址：https://github.com/liuhr/my2sql/  
增强版地址(本仓库)：https://github.com/fulldb/my2sql

增强版为自己修改版

```
0，一步到位，不需要再使用mysqlbinlog等工具查看sql  binlog的pos点。
按照二分查找法，读取少量binlog文件的开头来获取最终需要读取的文件。
1，根据时间回滚时，只读取需要的binlog文件（原版是从第一个binlog文件读取）
2，打印扩展信息到生成的sql文件中（添加gtid和原始sql文本）
3，增加支持根据某个gtid回滚sql
4，增加支持根据sql文本模糊匹配过滤
5，MySQL binlog压缩支持,初步支持binlog_transaction_compression=on

使用方法几乎和原版本类似，只是增加了两个参数

-sqltext 表示要模糊匹配的文本
-gtid 表示只生成这个gtid中的dml 
```

注意事项：

```
如果数据库名和表名不区分大小写，数据库名和表名要小写
开始时间最好减去1分钟，结束时间最好加上一分钟
sql文本最好能够唯一表示对应的sql。

```

对开源库github.com/go-mysql-org/go-mysql
vendor/github.com/go-mysql-org/go-mysql/replication/parser.go
增加全局函数
```

// added by momo
func (p *BinlogParser) ParseHeader(data []byte) (*EventHeader, error) {
	return p.parseHeader(data)
}

// added by momo
func (p *BinlogParser) ParseEvent(h *EventHeader, data []byte, rawData []byte) (Event, error) {
	return p.parseEvent(h, data, rawData)
}

```

# my2sql
go版MySQL binlog解析工具，通过解析MySQL binlog ，可以生成原始SQL、回滚SQL、去除主键的INSERT SQL等，也可以生成DML统计信息。类似工具有binlog2sql、MyFlash、my2fback等，本工具基于my2fback、binlog_rollback工具二次开发而来。


# 用途
* 数据快速回滚(闪回)
* 主从切换后新master丢数据的修复
* 从binlog生成标准SQL，带来的衍生功能
* 生成DML统计信息，可以找到哪些表更新的比较频繁
* IO高TPS高， 查出哪些表在频繁更新
* 找出某个时间点数据库是否有大事务或者长事务
* 主从延迟，分析主库执行的SQL语句
* 除了支持常规数据类型，对大部分工具不支持的数据类型做了支持，比如json、blob、text、emoji等数据类型sql生成


# 产品性能对比
binlog2sql当前是业界使用最广泛的MySQL回滚工具，下面对my2sql和binlog2sql做个性能对比。

|                          |my2sql     |binlog2sql|
|---                       |---         |---   |
|1.1G binlog生成回滚SQL      |  1分40秒   |    65分钟  |
|1.1G binlog生成原始SQL      |  1分30秒   |     50分钟|
|1.1G binlog生成表DML统计信息、以及事务统计信息     |   40秒     |不支持|

# 快速开始
### 执行闪回操作具体操作流程
[https://blog.csdn.net/liuhanran/article/details/107426162](https://blog.csdn.net/liuhanran/article/details/107426162)
### 解析binlog生成标准SQL
[https://blog.csdn.net/liuhanran/article/details/107427204](https://blog.csdn.net/liuhanran/article/details/107427204)
### 解析binlog 统计DML、长事务与大事务分析
[https://blog.csdn.net/liuhanran/article/details/107427391](https://blog.csdn.net/liuhanran/article/details/107427391)



# 重要参数说明
-U	
```
优先使用unique key作为where条件，默认false
```

-mode
```
repl: 伪装成从库解析binlog文件，file: 离线解析binlog文件, 默认repl
```
-local-binlog-file
```
当指定-mode=file 参数时，需要指定-local-binlog-file binlog文件相对路径或绝对路径,可以连续解析多个binlog文件，只需要指定起始文件名，程序会自动持续解析下个文件
```

-add-extraInfo
```
是否把database/table/datetime/binlogposition...信息以注释的方式加入生成的每条sql前，默认false
```
```
# datetime=2020-07-16_10:44:09 database=orchestrator table=cluster_domain_name binlog=mysql-bin.011519 startpos=15552 stoppos=15773
UPDATE `orchestrator`.`cluster_domain_name` SET `last_registered`='2020-07-16 10:44:09' WHERE `cluster_name`='192.168.1.1:3306'
```
-big-trx-row-limit n

```
transaction with affected rows greater or equal to this value is considerated as big transaction 
找出满足n条sql的事务，默认500条
```

-databases 、 -tables
```
库及表条件过滤, 以逗号分隔
```

-sql
```
要解析的sql类型，可选参数insert、update、delete，默认全部解析
```

-doNotAddPrifixDb

```
Prefix table name witch database name in sql,ex: insert into db1.tb1 (x1, x1) values (y1, y1)
默认生成insert into db1.tb1 (x1, x1) values (y1, y1)类sql，也可以生成不带库名的sql
```

-file-per-table
```
为每个表生成一个sql文件
```

-full-columns
```
For update sql, include unchanged columns. for update and delete, use all columns to build where condition.
default false, this is, use changed columns to build set part, use primary/unique key to build where condition
生成的sql是否带全列信息，默认false
```
-ignorePrimaryKeyForInsert
```
生成的insert语句是否去掉主键，默认false
```

-output-dir
```
将生成的结果存放到制定目录
```

-output-toScreen
```
将生成的结果打印到屏幕，默认写到文件
```

-threads
```
线程数，默认8个
```

-work-type
```
2sql：生成原始sql，rollback：生成回滚sql，stats：只统计DML、事务信息
```
-sqltext 
```
表示要模糊匹配的文本,目前不支持正则表达式
```

-gtid
```
一个gtid,目前只支持一个gtid
```











# 使用案例
### 解析出标准SQL
#### 根据时间点解析出标准SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl -work-type 2sql  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode file -local-binlog-file ./mysql-bin.011259  -work-type 2sql  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
```

#### 根据pos点解析出标准SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl  -work-type 2sql  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode file -local-binlog-file ./mysql-bin.011259  -work-type 2sql  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir
```

### 解析出回滚SQL
#### 根据时间点解析出回滚SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl -work-type rollback  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode file -local-binlog-file ./mysql-bin.011259 -work-type rollback  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00" -output-dir ./tmpdir
```

#### 根据pos点解析出回滚SQL
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl -work-type rollback  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306   -mode file -local-binlog-file ./mysql-bin.011259  -work-type rollback  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -output-dir ./tmpdir

```

### 统计DML以及大事务
#### 统计时间范围各个表的DML操作数量，统计一个事务大于500条、时间大于300秒的事务
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode repl -work-type stats  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00"  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode file -local-binlog-file ./mysql-bin.011259   -work-type stats  -start-file mysql-bin.011259  -start-datetime "2020-07-16 10:20:00" -stop-datetime "2020-07-16 11:00:00"  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
```

#### 统计一段pos点范围各个表的DML操作数量，统计一个事务大于500条、时间大于300秒的事务
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306  -mode repl -work-type stats  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
#直接读取binlog文件解析
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode file -local-binlog-file ./mysql-bin.011259  -work-type stats  -start-file mysql-bin.011259  -start-pos 4 -stop-file mysql-bin.011259 -stop-pos 583918266  -big-trx-row-limit 500 -long-trx-seconds 300   -output-dir ./tmpdir
```


### 从某一个pos点解析出标准SQL，并且持续打印到屏幕
```
#伪装成从库解析binlog
./my2sql  -user root -password xxxx -host 127.0.0.1   -port 3306 -mode repl  -work-type 2sql  -start-file mysql-bin.011259  -start-pos 4   -output-toScreen 
```

# 下载二进制版本
 + 有编译好的linux版本(CentOS release 7.x)  [点击下载Linux版](https://github.com/liuhr/my2sql/blob/master/releases/centOS_release_7.x/my2sql)

# 编译安装
```
cd $GOPATH/src
git clone https://github.com/liuhr/my2sql.git
cd my2sql/
go build .
```


# 限制
* 使用回滚/闪回功能时，binlog格式必须为row,且binlog_row_image=full， DML统计以及大事务分析不受影响
* 只能回滚DML， 不能回滚DDL
* 使用rollback功能时，要解析的binlog段，表结构要保持一致（例如：解析mysql-bin.000001文件，此binlog文件的的表有add column或drop column操作，则执行rollback可能会执行异常）
* 支持指定-tl时区来解释binlog中time/datetime字段的内容。开始时间-start-datetime与结束时间-stop-datetime也会使用此指定的时区，
  但注意此开始与结束时间针对的是binlog event header中保存的unix timestamp。结果中的额外的datetime时间信息都是binlog event header中的unix
timestamp
* 此工具是伪装成从库拉取binlog，需要连接数据库的用户有SELECT, REPLICATION SLAVE, REPLICATION CLIENT权限
* MySQL8.0版本需要在配置文件中加入default_authentication_plugin  =mysql_native_password，用户密码认证必须是mysql_native_password才能解析

# 感谢
 感谢[https://github.com/siddontang](https://github.com/siddontang)的binlog解析库， 感谢dropbox的sqlbuilder库，感谢my2fback、binlog_rollback

# TODO
- [x] GTID事务为单位进行解析
- [x] 闪回、回滚添加begin/commit事务标示
