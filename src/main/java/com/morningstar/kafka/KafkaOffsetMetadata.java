package com.morningstar.kafka;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;


public class KafkaOffsetMetadata {

	private final long KAFKA_OFFSET_METADATA_OFFSET_DEFAULT_VALUE = -1;
	private final long KAFKA_OFFSET_METADATA_TIMESTAMP_DEFAULT_VALUE = -1;

	@Expose private long offset;
	@Expose private long timestamp;


	public KafkaOffsetMetadata(long offset, long timestamp) {
		createObject(offset, timestamp);
	}

	public KafkaOffsetMetadata(long offset) {
		createObject(offset, KAFKA_OFFSET_METADATA_TIMESTAMP_DEFAULT_VALUE);
	}

	public KafkaOffsetMetadata() {
		createObject(KAFKA_OFFSET_METADATA_OFFSET_DEFAULT_VALUE, KAFKA_OFFSET_METADATA_TIMESTAMP_DEFAULT_VALUE);
	}

	private void createObject(long offset, long timestamp) {
		Preconditions.checkArgument(offset >= -1, "offset must be greater than or equal to -1.");
		Preconditions.checkArgument(timestamp >= -1, "timestamp must be greater than or equal to -1.");

		this.offset = offset;
		this.timestamp = timestamp;
	}


	public long getOffset() {
		return offset;
	}

	public long getTimestamp() {
		return timestamp;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KafkaOffsetMetadata that = (KafkaOffsetMetadata) o;

		if (getOffset() != that.getOffset()) return false;
		return getTimestamp() == that.getTimestamp();
	}

	@Override
	public int hashCode() {
		int result = (int) (getOffset() ^ (getOffset() >>> 32));
		result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
		return result;
	}
}
