class HyperTableCell
{
	constructor(client,namespace)
	{
		this.client = client;
		this.namespace = namespace;
	}

	set_cells(tableName,cells)
	{
		return new Promise((resolve,reject) => {
			this.client.set_cells(this.namespace, tableName, cells,function(error){reject(error)});
			resolve(true);
		});
	}

	get_cells(tableName,ScanSpec)
	{
		return new Promise((resolve,reject) => {
			this.client.get_cells(this.namespace, tableName, ScanSpec,
	        function (error, cells) {
	        	if (error) reject(error);
	        	resolve(cells);
	      	});
		});
	}

	set_cells_as_arrays(tableName,cells_as_arrays)
	{
		return new Promise((resolve,reject) => {
			this.client.set_cells_as_arrays(this.namespace,tableName,cells_as_arrays,resolve(true));
		});
	}

	get_cells_as_arrays(tableName,ScanSpec)
	{
		return new Promise((resolve,reject) => {
			this.client.get_cells_as_arrays(this.namespace, tableName, ScanSpec,
	        function (error, cells_as_arrays) {
	        	if (error) reject(error);
	        	resolve(cells_as_arrays);
	      	});
		});
	}

	set_cells_serialized(tableName,buffer)
	{
		return new Promise((resolve,reject) => {
			this.client.set_cells_serialized(this.namespace, tableName, buffer, resolve(true));
		});
	}

	get_cells_serialized(tableName,ScanSpec)
	{
		return new Promise((resolve,reject) => {
			this.client.get_cells_serialized(this.namespace, tableName, ScanSpec, 
			function (error, buffer){
				if (error) reject(error);
				resolve(buffer);
			});
		});
	}
}

module.exports = HyperTableCell;