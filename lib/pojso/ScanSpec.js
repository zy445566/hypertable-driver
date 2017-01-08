var hypertable = require('hypertable');
var HypertableObj = require('./HypertableObj');
class ScanSpec extends HypertableObj
{
	constructor()
	{
		super();
	}

	setColumns(columns)
	{
		this.data.columns = columns;
		return this;
	}

	setRowIntervals()
	{
		this.data.row_intervals = row_intervals;
		return this;
	}

	setVersions(versions)
	{
		this.data.versions = versions;
		return this;
	}

	setColumnPredicates(column_predicates)
	{
		this.data.column_predicates = column_predicates;
		return this;
	}

	setAndColumnPredicates(and_column_predicates)
	{
		this.data.and_column_predicates = and_column_predicates;
		return this;
	}

	setStartRow(start_row)
	{
		this.data.start_row = start_row;
		return this;
	}

	setEndRow(end_row)
	{
		this.data.end_row = end_row;
		return this;
	}

	setStartInclusive(start_inclusive)
	{
		this.data.start_inclusive = start_inclusive;
		return this;
	}

	setEndInclusive(end_inclusive)
	{
		this.data.end_inclusive = end_inclusive;
		return this;
	}

	getScanSpec()
	{
		return new hypertable.ScanSpec(this.data);
	}
}

module.exports = ScanSpec;