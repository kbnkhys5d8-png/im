package key

// 搜索隔离记录与代次状态使用独立空间，旧版正常队列扫描不会读到它们。
var TableSearchOutboxQuarantine = struct{ Id [2]byte }{Id: [2]byte{0x1C, 0x01}}
var TableSearchOutboxState = struct{ Id [2]byte }{Id: [2]byte{0x1D, 0x01}}

// 保留完整原始键作为后缀，损坏业务身份的记录也能无损隔离。
func NewSearchOutboxQuarantineKey(rawKey []byte) []byte {
	return append(NewSearchOutboxQuarantineLowKey(), rawKey...)
}

func NewSearchOutboxQuarantineLowKey() []byte {
	return []byte{TableSearchOutboxQuarantine.Id[0], TableSearchOutboxQuarantine.Id[1], dataTypeTable, 0}
}

func NewSearchOutboxQuarantineHighKey() []byte {
	return []byte{TableSearchOutboxQuarantine.Id[0], TableSearchOutboxQuarantine.Id[1], dataTypeTable, 1}
}

func NewSearchOutboxStateKey(rawKey []byte) []byte {
	return append([]byte{TableSearchOutboxState.Id[0], TableSearchOutboxState.Id[1], dataTypeTable, 0}, rawKey...)
}
