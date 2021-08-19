package ZipStreamer.record;

import java.util.Map;

public class Record {
    
    private Map<String, String> metadata;
    private Object data;
    
	public Map<String, String> getMetadata() {
		return metadata;
	}
	
	public void setMetadata(Map<String, String> metadata) {
		this.metadata = metadata;
	}
	
	public Object getData() {
		return data;
	}
	
	public void setData(Object data) {
		this.data = data;
	}

}
