package com.morningstar.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class KafkaCommittedOffset {

	private String groupName;
	private boolean groupIsActive;
	private String topicName;
	private int partitionId;
	private KafkaCommittedOffsetMetadata committedOffset;


	public KafkaCommittedOffset(String groupName, boolean groupIsActive, String topicName, int partitionId, long committedOffset, long committedMillis) {

		Preconditions.checkArgument(!Strings.isNullOrEmpty(groupName), "groupName must not be NULL or empty.");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(topicName), "topicName must not be NULL or empty.");
		Preconditions.checkArgument(partitionId > -1, "partitionId must be greater than or equal to 0.");
		Preconditions.checkArgument(committedOffset > -1, "committedOffset must be greater than or equal to 0.");
		Preconditions.checkArgument(committedMillis > -1, "committedMillis must be greater than or equal to 0.");

		this.groupName = groupName;
		this.groupIsActive = groupIsActive;
		this.topicName = topicName;
		this.partitionId = partitionId;
		this.committedOffset = new KafkaCommittedOffsetMetadata(committedOffset, committedMillis);
	}


	public String getGroupName() {
		return groupName;
	}

	public boolean getGroupIsActive() {
		return groupIsActive;
	}

	public String getTopicName() {
		return topicName;
	}

	public int getPartitionId() {
		return partitionId;
	}

	public KafkaCommittedOffsetMetadata getCommittedOffset() {
		return committedOffset;
	}
}
