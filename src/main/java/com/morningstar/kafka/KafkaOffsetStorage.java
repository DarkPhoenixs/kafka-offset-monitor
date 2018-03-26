package com.morningstar.kafka;


import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Optional;
import java.util.Set;


public class KafkaOffsetStorage {

	private Set<KafkaConsumerGroup> consumerGroups;
	private Set<KafkaTopicPartitionLogEndOffset> logEndOffsets;


	public KafkaOffsetStorage() {

		this.consumerGroups = Sets.newConcurrentHashSet();
		this.logEndOffsets = Sets.newConcurrentHashSet();
	}

	private Optional<KafkaConsumerGroup> getConsumerGroup(String consumerGroupName) {

		return consumerGroups.stream().filter(cg -> cg.getConsumerGroupName().equals(consumerGroupName)).findFirst();
	}

	private void upsertConsumerGroup(KafkaCommittedOffset kafkaCommittedOffset) {

		// Store committedOffset
		Optional<KafkaConsumerGroup> consumerGroup = getConsumerGroup(kafkaCommittedOffset.getGroupName());

		if (consumerGroup.isPresent()) {
			// Update existing group
			consumerGroup.get().addCommittedOffsetInfo(kafkaCommittedOffset);
		} else {
			// Create new group
			KafkaConsumerGroup newConsumerGroup = new KafkaConsumerGroup(kafkaCommittedOffset.getGroupName());
			newConsumerGroup.addCommittedOffsetInfo(kafkaCommittedOffset);
			consumerGroups.add(newConsumerGroup);
		}
	}

	private void addTopicPartition(String topicName, int partitionId) {

		KafkaTopicPartitionLogEndOffset incomingTopicPartition = new KafkaTopicPartitionLogEndOffset(
				new KafkaTopicPartition(topicName, partitionId));

		if (!logEndOffsets.contains(incomingTopicPartition)) {
			logEndOffsets.add(incomingTopicPartition);
		}
	}

	/* Calculate lag based on logEndOffset and committedOffset
	 *   Rule 1: If logEndOffset == -1, or committedOffset == -1, then lag is -1.
	 *           This is a timing issue and we will get caught up on the next go-around.
	 *   Rule 2: If committedOffset > logEndOffset, then lag is 0.
	 *           This is a timing issue where a committedOffset was reported before logEndOffset was updated.
	 *           We will get caught up on the next go-around.
	 *   Rule 3: Lag is logEndOffset - committedOffset
	 */
	private long calculateLag(long logEndOffset, long committedOffset) {

		if (logEndOffset == -1 || committedOffset == -1) {
			return -1;
		} else if (committedOffset > logEndOffset) {
			return 0;
		} else {
			return logEndOffset - committedOffset;
		}
	}

	private Optional<KafkaTopicPartitionLogEndOffset> getTopicPartitionLogEndOffset(String topicName, int partitionId) {

		return logEndOffsets.stream()
				.filter(leo -> (leo.getTopicPartition().getTopicName().equals(topicName) && leo.getTopicPartition().getPartitionId() == partitionId))
				.findFirst();
	}

	private KafkaOffsetMetadata getLogEndOffset(String topicName, int partitionId) {

		Optional<KafkaTopicPartitionLogEndOffset> topicPartitionLogEndOffset = getTopicPartitionLogEndOffset(topicName, partitionId);

		if (!topicPartitionLogEndOffset.isPresent()) {
			return new KafkaOffsetMetadata();
		} else {
			return topicPartitionLogEndOffset.get().getLogEndOffset();
		}
	}


	public String getConsumerGroups() {

		for (KafkaConsumerGroup consumerGroup : consumerGroups) {
			consumerGroup.updateStatus();
		}

		Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
		return gson.toJson(consumerGroups);
	}

	public Set<KafkaTopicPartitionLogEndOffset> getLogEndOffsets() {
		return logEndOffsets;
	}

	public void addCommittedOffset(KafkaCommittedOffset kafkaCommittedOffset) {

		addTopicPartition(kafkaCommittedOffset.getTopicName(), kafkaCommittedOffset.getPartitionId());

		// Calculate lag
		KafkaOffsetMetadata logEndOffsetMetadata = getLogEndOffset(kafkaCommittedOffset.getTopicName(), kafkaCommittedOffset.getPartitionId());
		long lag = calculateLag(logEndOffsetMetadata.getOffset(), kafkaCommittedOffset.getCommittedOffset().getOffset());

		// Add lag to committedOffset object
		kafkaCommittedOffset.getCommittedOffset().setLag(lag);

		// Store committedOffset
		upsertConsumerGroup(kafkaCommittedOffset);
	}

	public void addLogEndOffset(KafkaTopicPartitionLogEndOffset kafkaTopicLogEndOffset) {

		if (logEndOffsets.contains(kafkaTopicLogEndOffset)) {
			logEndOffsets.stream()
					.filter(leo -> leo.equals(kafkaTopicLogEndOffset))
					.findFirst().get()
					.setLogEndOffset(kafkaTopicLogEndOffset.getLogEndOffset());
		} else {
			logEndOffsets.add(kafkaTopicLogEndOffset);
		}
	}
}
