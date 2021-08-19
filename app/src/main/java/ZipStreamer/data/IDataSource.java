package ZipStreamer.data;

import ZipStreamer.record.Record;

public interface IDataSource extends IData {

	public Record getRecord();
	
}
