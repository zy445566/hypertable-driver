var hypertable = require('hypertable');
class Reader{
	constructor(buffer)
	{
		this.reader = new hypertable.SerializedCellsReader(buffer);
	}

	next()
	{
		return this.reader.next();
	}

	getCell()
	{
		return this.reader.getCell();
	}
}
module.exports = Reader;