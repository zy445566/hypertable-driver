class HyperTableTable{

	constructor(client,namespace)
	{
		this.client = client;
		this.namespace = namespace;
	}
	
	table_create(tableName,schema)
	{
		return new Promise((resolve,reject) => {
			this.client.table_create(this.namespace,tableName,schema,resolve(true));
		});
	}

	get_schema(tableName)
	{
		return new Promise((resolve,reject) => {
			this.client.get_schema(this.namespace,tableName,
			function(error,result){
				if (error) reject(error);
				resolve(result);
			});
		});
	}

	table_alter(tableName,schema)
	{
		return new Promise((resolve,reject) => {
			this.client.table_alter(this.namespace,tableName,schema,resolve(true));
		});
	}

	table_drop(tableName,ifExists=false)
	{
		return new Promise((resolve,reject) => {
			this.client.table_drop(this.namespace,tableName,ifExists,resolve(true));
		});
	}


}
module.exports = HyperTableTable;