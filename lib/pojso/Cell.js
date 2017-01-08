var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class Cell extends HypertableObj
{	
	constructor()
	{
		super();
	}
	
	setKey(key)
	{
		this.data.key = key;
		return this;
	}

	setValue(value)
	{
		this.data.value = value;
		return this;
	}

	getCell()
	{
		return new hypertable.Cell(this.data);
	}
}

module.exports = Cell;