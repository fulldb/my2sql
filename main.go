package main

import (
	"fmt"
	"sync"

	my "my2sql/base"
	"my2sql/findbinlog"

	//go get github.com/go-mysql-org/go-mysql@7c31dc4 ,下载较新的提交版本
	"github.com/go-mysql-org/go-mysql/replication"
)

func main() {
	my.GConfCmd.IfSetStopParsPoint = false
	my.GConfCmd.ParseCmdOptions()
	defer my.GConfCmd.CloseFH()
	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}
	var wg, wgGenSql sync.WaitGroup
	wg.Add(1)
	go my.ProcessBinEventStats(my.GConfCmd, &wg)

	if my.GConfCmd.WorkType != "stats" {
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			fmt.Println("开始-GenForwardRollbackSqlFromBinEvent", i)
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
			fmt.Println("结束-GenForwardRollbackSqlFromBinEvent", i)
		}
	}
	if my.GConfCmd.Mode == "repl" {
		//增加根据时间和文件开头过滤掉不需要解析的文件
		findbinlog.GetStartFileAndPos(my.GConfCmd)
		//开始获取binlog
		my.ParserAllBinEventsFromRepl(my.GConfCmd)
	} else if my.GConfCmd.Mode == "file" {
		myParser := my.BinFileParser{}
		myParser.Parser = replication.NewBinlogParser()
		// donot parse mysql datetime/time column into go time structure, take it as string
		myParser.Parser.SetParseTime(false)
		// sqlbuilder not support decimal type
		myParser.Parser.SetUseDecimal(false)
		myParser.MyParseAllBinlogFiles(my.GConfCmd)
	}
	wgGenSql.Wait()
	close(my.GConfCmd.SqlChan)
	wg.Wait()
}
