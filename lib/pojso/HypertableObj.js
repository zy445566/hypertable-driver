class HypertableObj
{
	constructor()
	{
		this.data = {};
	}

	getByName(name)
	{
		return this.data[name];
	}

	getData()
	{
		return this.data;
	}

	deleteByName(name)
	{
		delete this.data[name];
		return this;
	}
}
module.exports = HypertableObj;