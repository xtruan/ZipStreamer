package ZipStreamer.data;

import ZipStreamer.record.Record;

public class ConsoleDataSink implements IDataSink {

	@Override
	public void putRecord(Record record) {
		
		System.out.println(record.getData().toString());
		
		//System.out.println(
		//		new StringBuilder().append(record.getMetadata()).append(" -- ").append(record.getData()));
		
		//System.out.println("commas: " + record.getData().toString().split(",").length);
		
	}

}
