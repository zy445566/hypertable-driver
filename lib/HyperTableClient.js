var hypertable = require('hypertable');
class HyperTableClient {
	constructor(htConfig)
	{
		if (htConfig.timeout==null)
		{
			this.client = new hypertable.ThriftClient(htConfig.host, htConfig.port);
		} else {
			this.client = new hypertable.ThriftClient(htConfig.host, htConfig.port,htConfig.timeout);
		}
		
	}

	getClient()
	{
		return this.client;
	}
}
module.exports = HyperTableClient;