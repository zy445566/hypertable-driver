var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class ColumnPredicate extends HypertableObj
{	
	constructor()
	{
		super();
	}
	
	setColumnFamily(column_family)
	{
		this.data.column_family = column_family;
		return this;
	}

	setOperation(columnPredicateOperation)
	{
		this.data.operation = columnPredicateOperation;
		return this;
	}

	setValue(value)
	{
		this.data.value = value;
		return this;
	}
	
	getColumnPredicate()
	{
		return new hypertable.ColumnPredicate(this.data);
	}
}

module.exports = ColumnPredicate;