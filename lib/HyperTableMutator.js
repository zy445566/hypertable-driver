class HyperTableMutator
{
	constructor(client,namespace)
	{
		this.client = client;
		this.namespace = namespace;
	}

	mutator_open(tableName,flags,flush_interval)
	{
		return new Promise((resolve,reject) => {
			this.client.mutator_open(this.namespace,tableName,flags,flush_interval,
			function (error, mutator) {
				if (error) reject(error);
				resolve(mutator);
			});
		});
	}

	mutator_set_cells(mutator,cells)
	{
		return new Promise((resolve,reject) => {
			this.client.mutator_set_cells(mutator,cells,resolve(true));
		});
	}

	mutator_flush(mutator)
	{
		return new Promise((resolve,reject) => {
			this.client.mutator_flush(mutator,resolve(true));
		});
	}

	mutator_close(mutator)
	{
		return new Promise((resolve,reject) => {
			this.client.mutator_close(mutator,resolve(true));
		});
	}
}
module.exports = HyperTableMutator;