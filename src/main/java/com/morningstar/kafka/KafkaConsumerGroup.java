package com.morningstar.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.annotations.Expose;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


public class KafkaConsumerGroup {

	private final long COMPLETE_THRESHOLD = 10;

	@Expose private String consumerGroupName;
	@Expose private boolean isActive;
	@Expose private boolean complete;
	@Expose private long mostRecentCommittedMillis;
	@Expose private Status status;
	@Expose private Set<KafkaTopicPartition> topicPartitions;


	public KafkaConsumerGroup(String consumerGroupName) {

		Preconditions.checkArgument(!Strings.isNullOrEmpty(consumerGroupName), "consumerGroupName cannot be NULL or empty.");

		this.consumerGroupName = consumerGroupName;
		this.isActive = false;
		this.complete = false;
		this.mostRecentCommittedMillis = -1;
		this.status = Status.OK;
		this.topicPartitions = Sets.newConcurrentHashSet();
	}


	public String getConsumerGroupName() {
		return consumerGroupName;
	}

	public boolean getComplete() { return complete; }

	public long getMaxCommitedMillis() {
		return mostRecentCommittedMillis;
	}

	public boolean isActive() {
		return isActive;
	}

	public Set<KafkaTopicPartition> getTopicPartitions() {

		return topicPartitions;
	}

	public Set<String> getTopics() {

		return topicPartitions.stream()
				.map(KafkaTopicPartition::getTopicName)
				.collect(Collectors.toSet());
	}

	public synchronized void updateStatus() {

		if (!isActive) {
			this.status = Status.ERR;
			return;
		}

		Status newStatus = Status.OK;

		for (KafkaTopicPartition topicPartition : topicPartitions) {

			// Set group status to ERROR if any topicPartition's status is STOP
			if (Status.STOP == topicPartition.getStatus()) {
				newStatus = Status.ERR;
				break;
			}

			// Set group status to ERROR if any topicPartition's status is REWIND
			if (Status.REWIND == topicPartition.getStatus()) {
				newStatus = Status.ERR;
				break;
			}

			// Set group status to ERROR if any topicPartition's status is STALL
			if (Status.STALL == topicPartition.getStatus()) {
				newStatus = Status.ERR;
				break;
			}

			// Set group status to WARN if any topicPartition's status is WARN
			if (Status.WARN == topicPartition.getStatus()) {
				newStatus = Status.WARN;
				break;
			}
		}

		this.status = newStatus;
	}


	private Optional<KafkaTopicPartition> getTopicPartition(String topic, int partitionId) {

		//return committedOffsets.keySet().stream().filter(tp -> (tp.getTopicName().equals(topic) && tp.getPartitionId() == partitionId)).findFirst();
		return topicPartitions.stream()
				.filter(tp -> (tp.getTopicName().equals(topic) && tp.getPartitionId() == partitionId))
				.findFirst();
	}

	private void upsertTopicPartition(KafkaCommittedOffset kafkaCommittedOffset) {

		Preconditions.checkArgument(!Strings.isNullOrEmpty(kafkaCommittedOffset.getTopicName()), "topic cannot be NULL or empty.");
		Preconditions.checkArgument(kafkaCommittedOffset.getPartitionId() >= 0, "partitionId must be greater-than or equal-to zero.");

		String incomingTopicName = kafkaCommittedOffset.getTopicName();
		int incomingPartitionId = kafkaCommittedOffset.getPartitionId();

		Optional<KafkaTopicPartition> existingTopicPartition = getTopicPartition(incomingTopicName, incomingPartitionId);

		if (existingTopicPartition.isPresent()) {
			// Append committed offset info to existing set item
			existingTopicPartition.get().addCommittedOffset(kafkaCommittedOffset.getCommittedOffset());
		} else {
			// Add a new entry to the map
			KafkaTopicPartition newTopicPartition = new KafkaTopicPartition(incomingTopicName, incomingPartitionId);
			newTopicPartition.addCommittedOffset(kafkaCommittedOffset.getCommittedOffset());
			topicPartitions.add(newTopicPartition);
		}
	}

	private void setMostRecentCommittedMillis(long mostRecentCommittedMillis) {
		if (this.mostRecentCommittedMillis < mostRecentCommittedMillis) {
			this.mostRecentCommittedMillis = mostRecentCommittedMillis;
		}
	}

	private void updateCompleteFlag() {

		this.complete = topicPartitions.stream()
				.noneMatch(f -> f.getCommittedOffsets().size() < COMPLETE_THRESHOLD);
	}

	public void addCommittedOffsetInfo(KafkaCommittedOffset kafkaCommittedOffset) {

		setMostRecentCommittedMillis(kafkaCommittedOffset.getCommittedOffset().getTimestamp());
		this.isActive = kafkaCommittedOffset.getGroupIsActive();
		upsertTopicPartition(kafkaCommittedOffset);
		updateCompleteFlag();
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KafkaConsumerGroup that = (KafkaConsumerGroup) o;

		return getConsumerGroupName().equals(that.getConsumerGroupName());
	}

	@Override
	public int hashCode() {
		return getConsumerGroupName().hashCode();
	}
}
