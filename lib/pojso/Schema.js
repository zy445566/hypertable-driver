var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class Schema extends HypertableObj
{
	constructor()
	{
		super();
	}

	setAccessGroups(access_groups)
	{
		this.data.access_groups = access_groups;
		return this;
	}

	setColumnFamilies(column_families)
	{
		this.data.column_families = column_families;
		return this;
	}

	setAccessGroupDefaults(access_group_defaults)
	{
		this.data.access_group_defaults = access_group_defaults;
		return this;
	}

	setColumnFamilyDefaults(column_family_defaults)
	{
		this.data.column_family_defaults = column_family_defaults;
		return this;
	}

	getSchema()
	{
		return new hypertable.Schema(this.data);
	}
}
module.exports = Schema;