package findbinlog

import (
	"context"
	"fmt"
	"time"

	my "my2sql/base"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/spf13/cast"
)

func GetStartFileAndPos(conf *my.ConfCmd) (err error) {
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	if conf.IfSetStartFilePos || my.GConfCmd.Mode != "repl" || !conf.IfSetStartDateTime {
		return
	}
	//获取所有binlog名称和大小
	mybinlogs, err := GetAllBinaryLogsName(conf)
	if err != nil {
		fmt.Println("获取所有binary log名称和大小失败", err)
		return
	}

	//重写使用二分查找法查找开始的文件，提高效率
	var (
		start_num       int = 0
		end_num         int = 0
		mid_num         int = 0
		start_timestamp uint32
		end_timestamp   uint32
		mid_timestamp   uint32
	)

	end_num = len(mybinlogs) - 1
	if end_num-start_num > 2 {
		start_timestamp, err = GetOneBinaryLogStartTimestamp(conf, mybinlogs[start_num].Name)
		if err != nil {
			return err
		}
		mybinlogs[start_num].StartTimestamp = start_timestamp
		fmt.Println("#################################### 第一个文件", mybinlogs[start_num].Name, start_timestamp, "配置开始时间", time.Unix(int64(conf.StartDatetime), 0).Format("2006-01-02 15:04:05"), "文件开始时间", time.Unix(int64(start_timestamp), 0).Format("2006-01-02 15:04:05"))
		end_timestamp, err = GetOneBinaryLogStartTimestamp(conf, mybinlogs[end_num].Name)
		if err != nil {
			return err
		}
		mybinlogs[end_num].StartTimestamp = end_timestamp
		fmt.Println("####################################最后一个文件", mybinlogs[end_num].Name, end_timestamp, "配置开始时间", time.Unix(int64(conf.StartDatetime), 0).Format("2006-01-02 15:04:05"), "文件开始时间", time.Unix(int64(end_timestamp), 0).Format("2006-01-02 15:04:05"))
		//如果最后一个开始时间大于设置的开始时间
		if end_timestamp <= conf.StartDatetime {
			conf.StartFile = mybinlogs[end_num].Name
			fmt.Println("####################################最终开始文件是最后一个文件", conf.StartFile)
			return
		}
		if start_timestamp >= conf.StopDatetime {
			fmt.Println("####################################对应binlog可能已经删除,或时间选择不正确")
			return
		}
		for {
			if end_num-start_num == 1 {
				if end_timestamp <= conf.StartDatetime {
					conf.StartFile = mybinlogs[end_num].Name
				} else {
					conf.StartFile = mybinlogs[start_num].Name
				}
				fmt.Println("####################################最终开始文件是", conf.StartFile)
				return
			}
			mid_num = start_num + (end_num-start_num)/2
			mid_timestamp, err = GetOneBinaryLogStartTimestamp(conf, mybinlogs[mid_num].Name)
			if err != nil {
				return err
			}
			mybinlogs[mid_num].StartTimestamp = mid_timestamp
			if mid_timestamp < conf.StartDatetime {
				start_num = mid_num
				start_timestamp = mid_timestamp
			} else {
				end_num = mid_num
				end_timestamp = mid_timestamp
			}
			fmt.Println("####################################", mybinlogs[mid_num].Name, mid_timestamp, "配置开始时间", time.Unix(int64(conf.StartDatetime), 0).Format("2006-01-02 15:04:05"), "文件开始时间", time.Unix(int64(mid_timestamp), 0).Format("2006-01-02 15:04:05"))
		}
	}

	// for k, v := range mybinlogs {
	// 	timestamp, err := GetOneBinaryLogStartTimestamp(conf, v.Name)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	fmt.Println("####################################", v.Name, timestamp, "配置开始时间", time.Unix(int64(conf.StartDatetime), 0).Format("2006-01-02 15:04:05"), "文件开始时间", time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05"))
	// 	time.Sleep(10 * time.Microsecond)

	// 	if k == 0 && conf.StartDatetime <= timestamp {
	// 		conf.StartFile = v.Name
	// 		conf.StartPos = 0
	// 		fmt.Println("####################################最终开始文件", conf.StartFile)
	// 		break
	// 	}
	// 	if k > 0 {
	// 		mybinlogs[k].StartTimestamp = timestamp
	// 		mybinlogs[k-1].NextStartTimestamp = timestamp
	// 		if conf.StartDatetime <= timestamp {
	// 			conf.StartFile = mybinlogs[k-1].Name
	// 			conf.StartPos = 0
	// 			fmt.Println("####################################最终开始文件", conf.StartFile)
	// 			break
	// 		}
	// 	}
	// }
	// if conf.StartFile == "" {
	// 	conf.StartFile = mybinlogs[len(mybinlogs)-1].Name
	// 	fmt.Println("####################################最终开始文件是最后一个文件", conf.StartFile)
	// }
	return
}

// 获取binary log 开始时间戳
func GetOneBinaryLogStartTimestamp(conf *my.ConfCmd, name string) (timestamp uint32, err error) {

	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     conf.Host,
		Port:     uint16(conf.Port),
		User:     conf.User,
		Password: conf.Passwd,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	streamer, err := syncer.StartSync(mysql.Position{Name: name, Pos: 0})
	// streamer,err := syncer.StartSyncGTID(gtidSet)

	if err != nil {
		fmt.Println("连接到数据库失败", err)
		return
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			fmt.Println("获取binlog事件失败", err)
		}
		cancel()

		if err == context.DeadlineExceeded {
			// meet timeout
			break
		}
		// ev.Dump(os.Stdout)
		// fmt.Println(ev.Header.ServerID, ev.Header.LogPos, ev.Header.Timestamp)
		if ev.Header.Timestamp > 0 {
			timestamp = ev.Header.Timestamp
			return timestamp, err
		}
	}
	return
}

type MyBinlog struct {
	mysql.Position
	StartTimestamp     uint32
	NextStartTimestamp uint32
}

func GetAllBinaryLogsName(conf *my.ConfCmd) (mypos []MyBinlog, err error) {
	conn, err := client.Connect(conf.Host+":"+cast.ToString(conf.Port), conf.User, conf.Passwd, "")
	if err != nil {
		fmt.Println("连接到数据库失败", err)
	}
	defer conn.Close()
	conn.Ping()

	// Select
	r, err := conn.Execute(`show binary logs`)

	// Close result for reuse memory (it's not necessary but very useful)
	defer r.Close()

	mypos = make([]MyBinlog, 0)
	// Direct access to fields
	for _, row := range r.Values {
		var pos MyBinlog
		pos.Name = string(row[0].AsString())
		pos.Pos = uint32(row[1].AsInt64())
		mypos = append(mypos, pos)
	}
	return
}
