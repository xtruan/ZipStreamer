package ZipStreamer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import ZipStreamer.data.IDataSink;
import ZipStreamer.record.Record;

public class ZipReader {
	
	private static final boolean DEBUG_OUTPUT = true;
	private static final long DEBUG_FREQUENCY = 100000;
	
	private static final int BUFFER_SIZE = 1024;

	public void processFile(
			final String inFile, 
			final IDataSink dataSink, 
			final List<String> validExtensions, 
			final String recordSeparator) throws IOException {
		
		final long startTime = System.currentTimeMillis();
		long recordsProcessed = 0l;

		ZipInputStream zis = new ZipInputStream(new FileInputStream(inFile));

		StringBuilder partialRecordData = new StringBuilder();
		byte[] buffer = new byte[BUFFER_SIZE];
		int read = 0;
		ZipEntry entry;
		while ((entry = zis.getNextEntry()) != null) {
			final String entryName = entry.getName();
			
			if (validEntryName(entryName, validExtensions)) {
				while ((read = zis.read(buffer, 0, BUFFER_SIZE)) >= 0) {
					final String decoded = new String(buffer, 0, read, "UTF-8");
					final String[] decodedAndSplit = decoded.split(recordSeparator);
					final int numRecords = decodedAndSplit.length;
					
					if (decodedAndSplit.length > 0) {
						partialRecordData.append(decodedAndSplit[0]);
						publishRecord(dataSink, entryName, partialRecordData.toString());
						recordsProcessed++;
						if (DEBUG_OUTPUT) printDebugInfo(startTime, recordsProcessed);
						partialRecordData.setLength(0);
						partialRecordData.append(decodedAndSplit[numRecords - 1]);
					}
					
					for (int i = 1; i < numRecords - 1; i++) {
						publishRecord(dataSink, entry.getName(), decodedAndSplit[i]);
						recordsProcessed++;
						if (DEBUG_OUTPUT) printDebugInfo(startTime, recordsProcessed);
					}
					
				}
				
			}
		}

		zis.close();
	}
	
	private boolean validEntryName(final String name, final List<String> validExtensions) {
		if (validExtensions.isEmpty()) {
			return true;
		} else {
			final String lowerCaseName = name.toLowerCase();
			for (final String ext : validExtensions) {
				if (lowerCaseName.endsWith(ext)) {
					return true;
				}
			}
			return false;
		}
	}
	
	private void publishRecord(final IDataSink dataSink, final String name, final String textData) {
		Map<String, String> metadataMap = new HashMap<String, String>();
		metadataMap.put("name", name);
		
		Record record = new Record();
		record.setMetadata(metadataMap);
		record.setData(textData);
		
		dataSink.putRecord(record);
	}
	
	private void printDebugInfo(final long startTime, final long recordsProcessed) {
		if (recordsProcessed % DEBUG_FREQUENCY == 0) {
			final long currentTime = System.currentTimeMillis();
			final long elapsedTime = currentTime - startTime;
			final double elapsedTimeSec = elapsedTime / 1000.0;
			final long recordsPerSec = (recordsProcessed / elapsedTime) * 1000l;
			
			System.err.println(new StringBuilder()
					.append(elapsedTimeSec)
					.append(" -- records: ")
					.append(recordsProcessed)
					.append(", records/sec: ")
					.append(recordsPerSec).toString());
			
		}
	}

}
