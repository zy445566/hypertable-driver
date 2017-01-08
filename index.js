//pojso (Plain Ordinary JavaScript Object)
var AccessGroupOptions = require('./lib/pojso/AccessGroupOptions');
var AccessGroupSpec = require('./lib/pojso/AccessGroupSpec');
var Cell = require('./lib/pojso/Cell');
var ColumnFamilyOptions = require('./lib/pojso/ColumnFamilyOptions');
var ColumnFamilySpec = require('./lib/pojso/ColumnFamilySpec');
var ColumnPredicate = require('./lib/pojso/ColumnPredicate');
var ColumnPredicateOperation = require('./lib/pojso/ColumnPredicateOperation');
var Key = require('./lib/pojso/Key');
var KeyFlag = require('./lib/pojso/KeyFlag');
var Reader = require('./lib/pojso/Reader');
var RowInterval = require('./lib/pojso/RowInterval');
var ScanSpec = require('./lib/pojso/ScanSpec');
var Schema = require('./lib/pojso/Schema');
var Writer = require('./lib/pojso/Writer');
var Timestamp = require('./lib/pojso/Timestamp');
//Driver
var HyperTableCell = require('./lib/HyperTableCell');
var HyperTableClient = require('./lib/HyperTableClient');
var HyperTableHql = require('./lib/HyperTableHql');
var HyperTableMutator = require('./lib/HyperTableMutator');
var HyperTableNameSpace = require('./lib/HyperTableNameSpace');
var HyperTableScanner = require('./lib/HyperTableScanner');
var HyperTableTable = require('./lib/HyperTableTable');


var HyperTableDriver = {
	pojso :{
		AccessGroupOptions:AccessGroupOptions,
		AccessGroupSpec:AccessGroupSpec,
		Cell:Cell,
		ColumnFamilyOptions:ColumnFamilyOptions,
		ColumnFamilySpec:ColumnFamilySpec,
		ColumnPredicate:ColumnPredicate,
		ColumnPredicateOperation:ColumnPredicateOperation,
		Key:Key,
		KeyFlag:KeyFlag,
		Reader:Reader,
		RowInterval:RowInterval,
		ScanSpec:ScanSpec,
		Schema:Schema,
		Timestamp:Timestamp,
		Writer:Writer
	},
	HyperTableCell:HyperTableCell,
	HyperTableClient:HyperTableClient,
	HyperTableHql:HyperTableHql,
	HyperTableMutator:HyperTableMutator,
	HyperTableNameSpace:HyperTableNameSpace,
	HyperTableScanner:HyperTableScanner,
	HyperTableTable:HyperTableTable
}

module.exports = HyperTableDriver;