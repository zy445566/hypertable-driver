var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class ColumnFamilySpec extends HypertableObj
{
	constructor()
	{
		super();
	}

	setName(name)
	{
		this.data.name = name;
		return this;
	}

	setAccessGroup(access_group)
	{
		this.data.access_group = access_group;
		return this;
	}

	setValueIndex(value_index)
	{
		this.data.value_index = value_index;
		return this;
	}

	setQualifierIndex(qualifier_index)
	{
		this.data.qualifier_index = qualifier_index;
		return this;
	}

	setOptions(options)
	{
		this.data.options = options;
		return this;
	}

	getColumnFamilySpec()
	{
		return new hypertable.ColumnFamilySpec(this.data);
	}
}
module.exports = ColumnFamilySpec;