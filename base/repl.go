package base

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"github.com/spf13/cast"
)

func ParserAllBinEventsFromRepl(cfg *ConfCmd) {
	defer cfg.CloseChan()
	cfg.BinlogStreamer = NewReplBinlogStreamer(cfg)
	log.Info("start to get binlog from mysql")
	SendBinlogEventRepl(cfg)
	log.Info("finish getting binlog from mysql")
}

func NewReplBinlogStreamer(cfg *ConfCmd) *replication.BinlogStreamer {
	replCfg := replication.BinlogSyncerConfig{
		ServerID:                uint32(cfg.ServerId),
		Flavor:                  cfg.MysqlType,
		Host:                    cfg.Host,
		Port:                    uint16(cfg.Port),
		User:                    cfg.User,
		Password:                cfg.Passwd,
		Charset:                 "utf8",
		SemiSyncEnabled:         false,
		TimestampStringLocation: GBinlogTimeLocation,
		ParseTime:               false, //donot parse mysql datetime/time column into go time structure, take it as string
		UseDecimal:              false, // sqlbuilder not support decimal type
	}

	replSyncer := replication.NewBinlogSyncer(replCfg)

	syncPosition := mysql.Position{Name: cfg.StartFile, Pos: uint32(cfg.StartPos)}
	replStreamer, err := replSyncer.StartSync(syncPosition)
	if err != nil {
		log.Fatalf(fmt.Sprintf("error replication from master %s:%d %v", cfg.Host, cfg.Port, err))
	}
	return replStreamer
}

func SendBinlogEventRepl(cfg *ConfCmd) {
	var (
		err           error
		ev            *replication.BinlogEvent
		chkRe         int
		currentBinlog string = cfg.StartFile
		tbMapPos      uint32 = 0

		orgSql string = ""
		gtid   string = ""

		binEventIdx uint64 = 0
		trxIndex    uint64 = 0

		// binEventIdx uint64 = 0
		// trxIndex    uint64 = 0
		// trxStatus   int    = 0
		// sqlLower    string = ""

		// db      string = ""
		// tb      string = ""
		// sql     string = ""
		// orgSql  string = ""
		// sqlType string = ""
		// rowCnt  uint32 = 0

		// gtid string = ""

		//justStart   bool = true
		//orgSqlEvent *replication.RowsQueryEvent
	)
	for {

		if cfg.OutputToScreen {
			ev, err = cfg.BinlogStreamer.GetEvent(context.Background())
			if err != nil {
				log.Fatalf(fmt.Sprintf("error to get binlog event"))
				break
			}
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), EventTimeout)
			ev, err = cfg.BinlogStreamer.GetEvent(ctx)
			cancel()
			if err == context.Canceled {
				log.Infof("ready to quit! [%v]", err)
				break
			} else if err == context.DeadlineExceeded {
				log.Infof("deadline exceeded.")
				break
			} else if err != nil {
				log.Fatalf(fmt.Sprintf("error to get binlog event %v", err))
				break
			}
		}
		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize
			// avoid mysqlbing mask the row event as unknown table row event
		}
		raw_data_size := cast.ToInt64(len(ev.RawData))
		ev.RawData = []byte{} // we donnot need raw data

		oneMyEvent := &MyBinEvent{Gtid: gtid, OrgSql: orgSql, MyPos: mysql.Position{Name: currentBinlog, Pos: ev.Header.LogPos}, StartPos: tbMapPos, RawDataSize: raw_data_size}
		chkRe = oneMyEvent.CheckBinEvent(cfg, ev, &currentBinlog)

		if chkRe == C_reContinue {
			continue
		} else if chkRe == C_reBreak {
			break
		} else if chkRe == C_reFileEnd {
			continue
		} else if chkRe == C_rows {
			rows := ev.Event.(*replication.TransactionPayloadEvent)
			for _, ev2 := range rows.Events {
				raw_data_size := cast.ToInt64(len(ev2.RawData))
				ev2.RawData = make([]byte, 0)
				if ev2.Header.EventType == replication.TABLE_MAP_EVENT {
					tbMapPos = ev2.Header.LogPos - ev2.Header.EventSize
					// avoid mysqlbing mask the row event as unknown table row event
				}
				oneMyEvent2 := &MyBinEvent{Gtid: gtid, OrgSql: orgSql, MyPos: mysql.Position{Name: currentBinlog, Pos: ev2.Header.LogPos}, StartPos: tbMapPos, RawDataSize: raw_data_size}
				chkRe2 := oneMyEvent2.CheckBinEvent(cfg, ev2, &currentBinlog)
				if chkRe2 == C_reContinue {
					continue
				} else if chkRe2 == C_reBreak {
					break
				} else if chkRe2 == C_reFileEnd {
					continue
				}
				err = ParseBinlog(cfg, ev2, oneMyEvent2, currentBinlog, raw_data_size, tbMapPos, &trxIndex, &binEventIdx)
				if err != nil {
					log.Fatalf(fmt.Sprintf("error to parse TRANSACTION_PAYLOAD_EVENT binlog event %v", err))
					break
				}
				if oneMyEvent2.OrgSql != "" {
					orgSql = oneMyEvent2.OrgSql
				}
				if oneMyEvent2.Gtid != "" {
					gtid = oneMyEvent2.Gtid
				}
			}
		} else {
			err = ParseBinlog(cfg, ev, oneMyEvent, currentBinlog, raw_data_size, tbMapPos, &trxIndex, &binEventIdx)
			if err != nil {
				log.Fatalf(fmt.Sprintf("error to parse binlog event %v", err))
				break
			}
			if oneMyEvent.OrgSql != "" {
				orgSql = oneMyEvent.OrgSql
			}
			if oneMyEvent.Gtid != "" {
				gtid = oneMyEvent.Gtid
			}
		}
		// //输出
		// ev.Dump(os.Stdout)

		//if find := strings.Contains(db, "#"); find {
		//	log.Fatalf(fmt.Sprintf("Unsupported database name %s contains special character '#'", db))
		//	break
		//}
		//if find := strings.Contains(tb, "#"); find {
		//	log.Fatalf(fmt.Sprintf("Unsupported table name %s.%s contains special character '#'", db, tb))
		//	break
		//}

	}
}

func ParseBinlog(cfg *ConfCmd, ev *replication.BinlogEvent, oneMyEvent *MyBinEvent, currentBinlog string, raw_data_size int64, tbMapPos uint32, trxIndex, binEventIdx *uint64) (err error) {
	var (
		trxStatus int    = 0
		sqlLower  string = ""

		db      string = ""
		tb      string = ""
		sql     string = ""
		orgSql  string = ""
		sqlType string = ""
		rowCnt  uint32 = 0
		gtid    string = ""
	)

	db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(ev)

	if sqlType == "query" {
		sqlLower = strings.ToLower(sql)
		if sqlLower == "begin" {
			trxStatus = C_trxBegin
			*trxIndex++
		} else if sqlLower == "commit" {
			trxStatus = C_trxCommit
		} else if sqlLower == "rollback" {
			trxStatus = C_trxRollback
		} else if oneMyEvent.QuerySql != nil {
			trxStatus = C_trxProcess
			rowCnt = 1
		}

	} else if sqlType == "gtid" {
		gtid = sql
		oneMyEvent.Gtid = gtid
	} else if sqlType == "sql" {
		orgSql = sql
		oneMyEvent.OrgSql = orgSql
	} else {
		trxStatus = C_trxProcess
	}

	if oneMyEvent.OrgSql != "" && orgSql == "" {
		orgSql = oneMyEvent.OrgSql
	}
	if oneMyEvent.Gtid != "" && gtid == "" {
		gtid = oneMyEvent.Gtid
	}

	if cfg.WorkType != "stats" {
		ifSendEvent := false
		if oneMyEvent.IfRowsEvent {

			tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
				string(oneMyEvent.BinEvent.Table.Table))
			_, err = G_TablesColumnsInfo.GetTableInfoJson(string(oneMyEvent.BinEvent.Table.Schema),
				string(oneMyEvent.BinEvent.Table.Table))
			if err != nil {
				log.Fatalf(fmt.Sprintf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s",
					tbKey, oneMyEvent.MyPos.String()))
			}
			ifSendEvent = true
		}
		if sqlType == "sql" || sqlType == "gtid" {
			ifSendEvent = true
		}

		//根据gtid过滤binlog
		if cfg.Gtid != "" {
			if cfg.Gtid != gtid {
				ifSendEvent = false
			}
		}

		//根据sql文本过滤binlog
		if cfg.Sqltext != "" && ifSendEvent {
			if !strings.Contains(orgSql, cfg.Sqltext) {
				ifSendEvent = false
			}
		}

		if ifSendEvent {
			if oneMyEvent.IfRowsEvent {
				*binEventIdx++
			}
			oneMyEvent.EventIdx = *binEventIdx
			oneMyEvent.SqlType = sqlType
			oneMyEvent.Timestamp = ev.Header.Timestamp
			oneMyEvent.TrxIndex = *trxIndex
			oneMyEvent.TrxStatus = trxStatus
			cfg.EventChan <- *oneMyEvent
		}
	}

	//output analysis result whatever the WorkType is
	if sqlType != "" {
		if sqlType == "query" {
			cfg.StatChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: ev.Header.LogPos - ev.Header.EventSize, StopPos: ev.Header.LogPos,
				Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType, RawDataSize: raw_data_size}
		} else {
			cfg.StatChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: tbMapPos, StopPos: ev.Header.LogPos,
				Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType, RawDataSize: raw_data_size}
		}
	}
	return
}
