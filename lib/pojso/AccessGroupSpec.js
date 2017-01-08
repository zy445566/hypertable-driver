var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class AccessGroupSpec extends HypertableObj
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

	setOptions(options)
	{
		this.data.options = options;
		return this;
	}

	setDefaults(defaults)
	{
		this.data.defaults = defaults;
		return this;
	}

	getAccessGroupSpec()
	{
		return new hypertable.AccessGroupSpec(this.data);
	}
}
module.exports = AccessGroupSpec;