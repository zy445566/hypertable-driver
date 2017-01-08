class HyperTableNameSpace
{

	constructor(client)
	{
		this.client = client;
	}

	namespace_exists(namespaceName)
	{
		return new Promise((resolve,reject) => {
			this.client.namespace_exists(namespaceName, function (error, exists){
				if (error) reject(error);
				resolve(exists);
			});
		});
	}

	namespace_create(namespaceName)
	{
		return new Promise((resolve,reject) => {
			this.client.namespace_create(namespaceName,resolve(true));
		});
	}

	namespace_open(namespaceName)
	{
		return new Promise((resolve,reject) => {
			this.client.namespace_open(namespaceName,function (error, namespace) {
		        if (error) reject(error);
		        resolve(namespace);
		      });
		});
	}

	namespace_get_listing(namespace)
	{
	 	return new Promise((resolve,reject) => {
			this.client.namespace_get_listing(namespace,function (error, listing) {
		        if (error) reject(error);
		        resolve(listing);
		      });
		});
	}

	namespace_close(namespace)
	{
		return new Promise((resolve,reject) => {
			this.client.namespace_close(namespace,resolve(true));
		});
	}
}
module.exports = HyperTableNameSpace;