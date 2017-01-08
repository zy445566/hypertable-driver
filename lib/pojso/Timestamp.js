var hypertableTimestamp = require('hypertable/timestamp');
var HypertableObj = require('./HypertableObj');
class Timestamp extends HypertableObj
{
	constructor()
	{
		super();
		this.data.date = new Date();
	}

	setTime(millisecond)
	{
		this.data.date.setTime(millisecond);
		return this;
	}

	getTimestamp()
	{
		return new hypertableTimestamp(this.data.date);
	}

}
module.exports = Timestamp;