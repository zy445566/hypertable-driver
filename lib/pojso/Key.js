var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class Key extends HypertableObj
{
	constructor()
	{
		super();
	}

	setRow(row)
	{
		this.data.row = row;
		return this;
	}

	setColumnFamily(column_family)
	{
		this.data.column_family = column_family;
		return this;
	}

	setColumnQualifier(column_qualifier)
	{
		this.data.column_qualifier = column_qualifier;
		return this;
	}

	setTimestamp(timestamp)
	{
		this.data.timestamp = timestamp;
		return this;
	}

	setRevision(revision)
	{
		this.data.revision = revision;
		return this;
	}

	setFlag(flag)
	{
		this.data.flag = flag;
		return this;
	}

	getKey()
	{
		return new hypertable.Key(this.data);
	}
}

module.exports = Key;