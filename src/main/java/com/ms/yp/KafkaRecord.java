package com.ms.yp;

public class KafkaRecord {
	private int partition;
	private long timestamp;
	private String key;
	private String message;

	public int getPartition() {
		return partition;
	}
	public void setPartition(Integer partition) {
		this.partition = partition;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		if (timestamp == null) {
			this.timestamp = System.currentTimeMillis();
		} else {
			this.timestamp = timestamp.longValue();
		}
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
}
