package com.morningstar.kafka;

public class KafkaTopicPartitionLogEndOffset {

	private KafkaTopicPartition topicPartition;
	private KafkaOffsetMetadata logEndOffset;


	public KafkaTopicPartitionLogEndOffset(KafkaTopicPartition topicPartition, KafkaOffsetMetadata logEndOffset) {
		createObject(topicPartition, logEndOffset);
	}

	public KafkaTopicPartitionLogEndOffset(KafkaTopicPartition topicPartition) {
		createObject(topicPartition, new KafkaOffsetMetadata(-1));
	}

	private void createObject(KafkaTopicPartition topicPartition, KafkaOffsetMetadata offsetInfo) {
		this.topicPartition = topicPartition;
		this.logEndOffset = offsetInfo;
	}

	public KafkaTopicPartition getTopicPartition() {
		return topicPartition;
	}

	public KafkaOffsetMetadata getLogEndOffset() {
		return logEndOffset;
	}

	public void setLogEndOffset(KafkaOffsetMetadata logEndOffset) {
		this.logEndOffset = logEndOffset;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KafkaTopicPartitionLogEndOffset that = (KafkaTopicPartitionLogEndOffset) o;

		return topicPartition.equals(that.topicPartition);
	}

	@Override
	public int hashCode() {
		return topicPartition.hashCode();
	}
}
