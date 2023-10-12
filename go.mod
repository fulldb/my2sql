module my2sql

go 1.19

require (
	github.com/dropbox/godropbox v0.0.0-20200228041828-52ad444d3502
	github.com/go-mysql-org/go-mysql v1.7.0
	github.com/go-sql-driver/mysql v1.7.1
	github.com/google/uuid v1.3.0
	github.com/juju/errors v0.0.0-20220203013757-bd733f3c86b9
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed
	github.com/spf13/cast v1.5.1
)

require (
	github.com/juju/testing v1.0.2 // indirect
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63 // indirect
	github.com/shopspring/decimal v1.2.1-0.20200707070546-867ed12000cf // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	go.uber.org/atomic v1.7.0 // indirect
)

// replace github.com/go-mysql-org/go-mysql => github.com/liuhr/go-mysql v0.0.0-20221109130012-ad3338a67e8f

// // added by momo
// func (p *BinlogParser) ParseHeader(data []byte) (*EventHeader, error) {
// 	return p.parseHeader(data)
// }

// // added by momo
// func (p *BinlogParser) ParseEvent(h *EventHeader, data []byte, rawData []byte) (Event, error) {
// 	return p.parseEvent(h, data, rawData)
// }
