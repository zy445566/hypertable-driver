# hypertable-driver
hypertable-driver

# attention
node -v >=6.9.1
Use for hytable 0.9.8

# use 
npm install hypertable-driver

#website
www.hypertable.com

#api		
http://www.hypertable.com/documentation/reference_manual/thrift_api

# instructions & example

##HyperTableTable
```node
var namespace = null;
var hyperTableTable = null;
var hyperTableNameSpace = new HyperTableDriver.HyperTableNameSpace(client);

hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	var accessGroupOptions = new HyperTableDriver.pojso.AccessGroupOptions();
	var columnFamilyOptions = new HyperTableDriver.pojso.ColumnFamilyOptions();
	var accessGroupSpec = new HyperTableDriver.pojso.AccessGroupSpec();
	var columnFamilySpec = new HyperTableDriver.pojso.ColumnFamilySpec();

	var defaultAccessGroupOptions = accessGroupOptions.setBlocksize(65536)
	.getAccessGroupOptions();
	var defaultColumnFamilyOptions = columnFamilyOptions.setMaxVersions(3)
	.getColumnFamilyOptions();
	var accessGroups ={};
	var columnFamilies ={};

	var ag_normal = accessGroupSpec.setName('ag_normal')
	.setDefaults(defaultColumnFamilyOptions)
	.getAccessGroupSpec();
	accessGroups.ag_normal = ag_normal;


	var fastAccessGroupOptions = accessGroupOptions
	.setInMemory(true)
	.getAccessGroupOptions();

	var ag_fast = accessGroupSpec.setName('ag_fast')
	.setOptions(fastAccessGroupOptions)
	.getAccessGroupSpec();
	accessGroups.ag_fast = ag_fast;

	var secureAccessGroupOptions = accessGroupOptions
	.deleteByName('in_memory')
	.setReplication(5)
	.getAccessGroupOptions();

	var ag_secure = accessGroupSpec.setName('ag_secure')
	.setOptions(secureAccessGroupOptions)
	.getAccessGroupSpec();
	accessGroups.ag_secure = ag_secure;

	var name = columnFamilySpec.setName('name')
	.setAccessGroup('ag_normal')
	.setOptions(defaultColumnFamilyOptions)
	.getColumnFamilySpec();
	var age = columnFamilySpec.setName('age')
	.setAccessGroup('ag_fast')
	.getColumnFamilySpec();
	var sex = columnFamilySpec.setName('sex')
	.setAccessGroup('ag_secure')
	.getColumnFamilySpec();
	columnFamilies.name = name;
	columnFamilies.age = age;
	columnFamilies.sex = sex;

	var schema = new HyperTableDriver.pojso.Schema();
	var schemaObj = schema
	.setAccessGroups(accessGroups)
	.setColumnFamilies(columnFamilies)
	.setAccessGroupDefaults(defaultAccessGroupOptions)
	.setColumnFamilyDefaults(defaultColumnFamilyOptions)
	.getSchema();
	// console.log(schema);return;
	hyperTableTable = new HyperTableDriver.HyperTableTable(client,namespace);
	return hyperTableTable.table_create('testTable2',schemaObj);
})
.then((result)=>{
	console.log(result);
});

hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableTable = new HyperTableDriver.HyperTableTable(client,namespace);
	return hyperTableTable.get_schema('testTable2');
})
.then((schemaObj)=>{
	//modify 'age' to 'birthday'
	var cfSpec = schemaObj.column_families['age'];
    delete schemaObj.column_families['age'];
    cfSpec.name = 'birthday';
    schemaObj.column_families['birthday'] = cfSpec;

    //add 'city'
    var columnFamilySpec = new HyperTableDriver.pojso.ColumnFamilySpec();
    cfSpec = columnFamilySpec.setName('city')
    .setAccessGroup('ag_normal')
    .getColumnFamilySpec();
    schemaObj.column_families['city'] = cfSpec;

	return hyperTableTable.table_alter('testTable2',schemaObj);
})
.then((result)=>{
	console.log(result);
})
.catch((err) => {
  console.log(err);
});


hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableTable = new HyperTableDriver.HyperTableTable(client,namespace);
	//or hyperTableTable.table_drop('testTable2',ifExists=true);
	return hyperTableTable.table_drop('testTable2');
})
.then((result)=>{
	console.log(result);
})
```

##HyperTableCell
```node
var namespace = null;
var hyperTableCell = null;
var hyperTableNameSpace = new HyperTableDriver.HyperTableNameSpace(client);

hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableCell = new HyperTableDriver.HyperTableCell(client,namespace);
	var key = new HyperTableDriver.pojso.Key();
	var cell = new HyperTableDriver.pojso.Cell();
	var keyObj1 = key.setRow('user3').setColumnFamily('name').setColumnQualifier('column_qualifier1').getKey();
	var keyObj2 = key.setRow('user2').setColumnFamily('name').setColumnQualifier('column_qualifier2').getKey();
	var cells = [];
	var cellObj1 = cell.setKey(keyObj1).setValue('test1').getCell();
	var cellObj2 = cell.setKey(keyObj2).setValue('test2').getCell();
	cells.push(cellObj1);
	cells.push(cellObj2);
	return hyperTableCell.set_cells('user',cells);
})
.then((result)=>{
	console.log(result);
});

hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableCell = new HyperTableDriver.HyperTableCell(client,namespace);
	var scanSpec = new HyperTableDriver.pojso.ScanSpec();
	var scanSpecObj = scanSpec.setColumns(['name','age']).getScanSpec();
	return hyperTableCell.get_cells('user',scanSpecObj);
})
.then((result)=>{
	console.log(result);
});


hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableCell = new HyperTableDriver.HyperTableCell(client,namespace);
	var scanSpec = new HyperTableDriver.pojso.ScanSpec();
	var scanSpecObj = scanSpec.setColumns(['name','age']).getScanSpec();
	return hyperTableCell.get_cells('user',scanSpecObj);
})
.then((result)=>{
	console.log(result);
});


hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableCell = new HyperTableDriver.HyperTableCell(client,namespace);
	cells_as_arrays = [];
	cell_as_array = ['user4', 'name', 'column_qualifier3', 'zl'];
	cells_as_arrays.push(cell_as_array);
	cell_as_array = ['user4', 'age', '', '16'];
	cells_as_arrays.push(cell_as_array);
	return hyperTableCell.set_cells_as_arrays('user',cells_as_arrays);
})
.then((result)=>{
	console.log(result);
});

hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableCell = new HyperTableDriver.HyperTableCell(client,namespace);
	var scanSpec = new HyperTableDriver.pojso.ScanSpec();
	var scanSpecObj = scanSpec.setColumns(['name','age']).getScanSpec();
	return hyperTableCell.get_cells_as_arrays('user',scanSpecObj);
})
.then((result)=>{
	console.log(result);
});

hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableCell = new HyperTableDriver.HyperTableCell(client,namespace);
	var writer = new HyperTableDriver.pojso.Writer(1024);
	var timestamp = new HyperTableDriver.pojso.Timestamp();
	writer.add('user0', 'name', 'column_qualifier4',timestamp.setTime(1483758549000).getTimestamp(), 'zl');
	writer.add('user5', 'age', 'column_qualifier5', null, '24');

	var buffer = writer.getBuffer();
	return hyperTableCell.set_cells_serialized('user',buffer);
})
.then((result)=>{
	console.log(result);
})
.catch((err) => {
  console.log(err);
});

hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableCell = new HyperTableDriver.HyperTableCell(client,namespace);
	var scanSpec = new HyperTableDriver.pojso.ScanSpec();
	var scanSpecObj = scanSpec.setColumns(['name','age']).getScanSpec();
	return hyperTableCell.get_cells_serialized('user',scanSpecObj);
})
.then((result)=>{
	var reader = new HyperTableDriver.pojso.Reader(result);
	reader.next()
    while (reader.next())
        console.log(reader.getCell().toString());
});
```

##hyperTableMutator
```node
var namespace = null;
var hyperTableMutator = null;
var mutator = null;
var hyperTableNameSpace = new HyperTableDriver.HyperTableNameSpace(client);
hyperTableNameSpace.namespace_open('test')
.then((result)=>{
	namespace = result;
	hyperTableMutator = new HyperTableDriver.HyperTableMutator(client,namespace);
	var flags =0;
	var flush_interval = 0;
	return hyperTableMutator.mutator_open('user',flags,flush_interval);
})
.then((result)=>{
	mutator = result;
	var key = new HyperTableDriver.pojso.Key();
	var cell = new HyperTableDriver.pojso.Cell();
	var keyObj1 = key.setRow('user2').setColumnFamily('name')
	.setColumnQualifier('column_qualifier4')
	.getKey();
	var keyObj2 = key.setRow('user3').setColumnFamily('name')
	.deleteByName('timestamp')
	.setColumnQualifier('column_qualifier5')
	.getKey();
	var cells = [];
	var cellObj1 = cell.setKey(keyObj1).setValue('test1').getCell();
	var cellObj2 = cell.setKey(keyObj2).setValue('test2').getCell();
	cells.push(cellObj1);
	cells.push(cellObj2);
	return hyperTableMutator.mutator_set_cells(mutator,cells);
})
.then((result)=>{
	var key = new HyperTableDriver.pojso.Key();
	var cell = new HyperTableDriver.pojso.Cell();
	var keyObj1 = key.setRow('user2')
	.setFlag(HyperTableDriver.pojso.KeyFlag.DELETE_ROW)
	.getKey();
	var keyObj2 = key.setRow('user3').getKey();
	var cells = [];
	var cellObj1 = cell.setKey(keyObj1).getCell();
	var cellObj2 = cell.setKey(keyObj2).getCell();
	cells.push(cellObj1);
	cells.push(cellObj2);
	return hyperTableMutator.mutator_set_cells(mutator,cells);
})
.then((result)=>{
	console.log(result);
	return hyperTableMutator.mutator_flush(mutator);
})
.then((result)=>{
	console.log(result);
	return hyperTableMutator.mutator_close(mutator);
})
.then((result)=>{
	console.log(result);
})
.catch((err) => {
  console.log(err);
});
```

##HyperTableScanner
```node
var namespace = null;
var hyperTableScanner = null;
var scanner = null;
var hyperTableNameSpace = new HyperTableDriver.HyperTableNameSpace(client);
hyperTableNameSpace.namespace_open('zytest')
.then((result)=>{
	namespace = result;
	hyperTableScanner = new HyperTableDriver.HyperTableScanner(client,namespace);
	var scanSpec = new HyperTableDriver.pojso.ScanSpec();
	var scanSpecObj = scanSpec.setColumns(['name','age']).getScanSpec();
	return hyperTableScanner.scanner_open('user',scanSpecObj);
})
.then((result)=>{
	scanner = result;
	return hyperTableScanner.scanner_get_cells(scanner);
})
.then((result)=>{
	console.log(result);
	return hyperTableScanner.scanner_close(scanner);
})
.then((result)=>{
	console.log(result);
});
```

##HyperTableHql
```node
var namespace = null;
var hyperTableHql = null;
var hyperTableMutator = null;
var mutator = null;
var hyperTableScanner = null;
var scanner = null;
var hyperTableNameSpace = new HyperTableDriver.HyperTableNameSpace(client);
hyperTableNameSpace.namespace_open('zytest')
.then((result)=>{
	namespace = result;
	hyperTableHql = new HyperTableDriver.HyperTableHql(client,namespace);
	var hql = "GET LISTING";
	return hyperTableHql.hql_query(hql);
})
.then((result)=>{
	console.log(result);
	var hql = "SELECT * from user WHERE ROW = 'user1'";
	return hyperTableHql.hql_query(hql);
})
.then((result)=>{
	console.log(result);
	var hql = "INSERT INTO user VALUES('user1','age','21'),('user2','age','22')";
	return hyperTableHql.hql_exec(hql);
})
// hql_exec(mutator)
.then((result)=>{
	console.log(result);
	var hql = "INSERT INTO user VALUES('user3','age','21'),('user4','age','22')";
	return hyperTableHql.hql_exec(hql,true);
})
.then((result)=>{
	console.log(result);
	mutator = result.mutator;
	hyperTableMutator = new HyperTableDriver.HyperTableMutator(client,namespace);
	var key = new HyperTableDriver.pojso.Key();
	var cell = new HyperTableDriver.pojso.Cell();
	var keyObj1 = key.setRow('user2').setColumnFamily('name')
	.setColumnQualifier('column_qualifier4')
	.getKey();
	var keyObj2 = key.setRow('user3').setColumnFamily('name')
	.deleteByName('timestamp')
	.setColumnQualifier('column_qualifier5')
	.getKey();
	var cells = [];
	var cellObj1 = cell.setKey(keyObj1).setValue('test1').getCell();
	var cellObj2 = cell.setKey(keyObj2).setValue('test2').getCell();
	cells.push(cellObj1);
	cells.push(cellObj2);
	return hyperTableMutator.mutator_set_cells(mutator,cells);
})
.then((result)=>{
	console.log(result);	
	return hyperTableMutator.mutator_flush(mutator);
})
.then((result)=>{
	console.log(result);	
	return hyperTableMutator.mutator_close(mutator);
})
// hql_exec(scanner)
.then((result)=>{
	console.log(result);	
	var hql = "SELECT * from user WHERE ROW = 'user3'";
	return hyperTableHql.hql_exec(hql,false,true);
})
.then((result)=>{
	console.log(result);
	scanner = result.scanner;
	hyperTableScanner = new HyperTableDriver.HyperTableScanner(client,namespace);
	return hyperTableScanner.scanner_get_cells(scanner);
})
.then((result)=>{
	console.log(result);
	return hyperTableScanner.scanner_close(scanner);
})
.then((result)=>{
	console.log(result);
	return hyperTableScanner.scanner_close(scanner);
})
```









