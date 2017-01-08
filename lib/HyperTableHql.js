class HyperTableHql
{
	constructor(client,namespace)
	{
		this.client = client;
		this.namespace = namespace;
	}

	hql_query(hql)
	{
		return new Promise((resolve,reject) => {
			this.client.hql_query(this.namespace,hql,
			function (error, response) {
				if (error) reject(error);
				resolve(response);
			});
		});
	}

	hql_query_as_arrays(hql)
	{
		return new Promise((resolve,reject) => {
			this.client.hql_query_as_arrays(this.namespace,hql,
			function (error, result) {
				if (error) reject(error);
				resolve(result);
			});
		});
	}

	hql_exec(hql,noflush=false,unbuffered=false)
	{
		return new Promise((resolve,reject) => {
			this.client.hql_exec(this.namespace,hql,noflush,unbuffered,
			function (error, result) {
				if (error) reject(error);
				resolve(result);
			});
		});
	}
}
module.exports = HyperTableHql;