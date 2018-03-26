package com.morningstar.kafka;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;

public class KafkaCommittedOffsetMetadata extends KafkaOffsetMetadata {

	@Expose private long lag = -1;


	public KafkaCommittedOffsetMetadata(KafkaOffsetMetadata offsetMetadata, long lag) {
		super(offsetMetadata.getOffset(), offsetMetadata.getTimestamp());
		verifyParameters(lag);
		this.lag = lag;
	}

	public KafkaCommittedOffsetMetadata(long committedOffset, long timestamp, long lag) {
		super(committedOffset, timestamp);
		verifyParameters(lag);
		this.lag = lag;
	}

	public KafkaCommittedOffsetMetadata(KafkaOffsetMetadata offsetMetadata) {
		super(offsetMetadata.getOffset(), offsetMetadata.getTimestamp());
	}

	public KafkaCommittedOffsetMetadata(long committedOffset, long timestamp) {
		super(committedOffset, timestamp);
	}

	private void verifyParameters(long lag) {

		Preconditions.checkArgument(lag > -2, "lag must not be less than -1.");
	}


	public long getLag() {
		return lag;
	}

	public void setLag(long lag) {
		this.lag = lag;
	}
}
