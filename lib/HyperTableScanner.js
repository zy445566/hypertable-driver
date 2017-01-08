class HyperTableScanner
{
	constructor(client,namespace)
	{
		this.client = client;
		this.namespace = namespace;
	}

	scanner_open(tableName,scanSpec)
	{
		return new Promise((resolve,reject) => {
			this.client.scanner_open(this.namespace,tableName,scanSpec,
			function (error, scanner) {
				if (error) reject(error);
				resolve(scanner);
			});
		});
	}

	scanner_get_cells(scanner)
	{
		return new Promise((resolve,reject) => {
			this.client.scanner_get_cells(scanner,
			function (error, result) {
				if (error) reject(error);
				resolve(result);
			});
		});
	}

	scanner_close(scanner)
	{
		return new Promise((resolve,reject) => {
			this.client.scanner_close(scanner,resolve(true));
		});
	}
}
module.exports = HyperTableScanner;