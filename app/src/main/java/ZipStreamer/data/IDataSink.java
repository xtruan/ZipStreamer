package ZipStreamer.data;

import ZipStreamer.record.Record;

public interface IDataSink extends IData {
	
	public void putRecord(final Record record);

}
