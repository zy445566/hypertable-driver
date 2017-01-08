var hypertable = require('hypertable');
class Writer{
	constructor(size)
	{
		this.writer = new hypertable.SerializedCellsWriter(size);
	}

	add(row,column_family=null,column_qualifier=null,timestamp=null,value=null,cell_flag=255)
	{
		this.writer.add(row,column_family,column_qualifier,timestamp,value,cell_flag);
	}
	getWrite()
	{
		return this.writer;
	}
	getBuffer()
	{
		return this.writer.getBuffer();
	}
}
module.exports = Writer;