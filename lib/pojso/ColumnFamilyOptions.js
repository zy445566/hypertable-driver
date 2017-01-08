var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class ColumnFamilyOptions extends HypertableObj
{
	constructor()
	{
		super();
	}

	setMaxVersions(max_versions)
	{
		this.data.max_versions = max_versions;
		return this;
	}

	setCounter(counter)
	{
		this.data.counter = counter;
		return this;
	}

	getColumnFamilyOptions()
	{
		return new hypertable.ColumnFamilyOptions(this.data);
	}
}
module.exports = ColumnFamilyOptions;