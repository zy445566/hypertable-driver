var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class AccessGroupOptions extends HypertableObj
{
	constructor()
	{
		super();
	}

	setBlocksize(blocksize)
	{
		this.data.blocksize = blocksize;
		return this;
	}

	setInMemory(in_memory)
	{
		this.data.in_memory = in_memory;
		return this;
	}

	setReplication(replication)
	{
		this.data.replication = replication;
		return this;
	}

	setCompressor(compressor)
	{
		this.data.compressor = compressor;
		return this;
	}

	setBloomFilter(bloom_filter)
	{
		this.data.bloom_filter = bloom_filter;
		return this;
	}

	getAccessGroupOptions()
	{
		return new hypertable.AccessGroupOptions(this.data);

	}
}
module.exports = AccessGroupOptions;