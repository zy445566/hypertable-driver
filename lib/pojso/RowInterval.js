var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class RowInterval extends HypertableObj
{	
	constructor()
	{
		super();
	}
	
	setStartRow(start_row)
	{
		this.data.start_row = start_row;
		return this;
	}

	setStartInclusive(start_inclusive)
	{
		this.data.start_inclusive = start_inclusive;
		return this;
	}

	setEndRow(end_row)
	{
		this.data.end_row = end_row;
		return this;
	}

	setEndInclusive(end_inclusive)
	{
		this.data.end_inclusive = end_inclusive;
		return this;
	}

	getRowInterval()
	{
		return new hypertable.RowInterval(this.data);
	}
}

module.exports = RowInterval;