package com.morningstar.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.EvictingQueue;
import com.google.gson.annotations.Expose;

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;


public class KafkaTopicPartition {

	private final int MAX_HISTORICAL_CONSUMER_OFFSETS = 10;

	private final Comparator<KafkaCommittedOffsetMetadata> committedOffsetLagCompare = Comparator.comparing(KafkaCommittedOffsetMetadata::getLag);
	private final Comparator<KafkaCommittedOffsetMetadata> committedOffsetTimestampCompare = Comparator.comparing(KafkaCommittedOffsetMetadata::getTimestamp);

	@Expose private String topicName;
	@Expose private long partitionId;
	@Expose private long currentLag;
	@Expose private Status status;

	@Expose private EvictingQueue<KafkaCommittedOffsetMetadata> committedOffsets;


	public KafkaTopicPartition(String topicName, long partitionId) {
		createObject(topicName, partitionId, -1, System.currentTimeMillis());
	}

	private void createObject(String topicName, long partitionId, long logEndOffset, long logEndOffsetMillis) {

		Preconditions.checkArgument(!Strings.isNullOrEmpty(topicName), "topicName must not be NULL or empty.");
		Preconditions.checkArgument(partitionId > -1, "partitionId must not be less than 0.");

		this.topicName = topicName;
		this.partitionId = partitionId;
		this.status = Status.OK;
		this.committedOffsets = EvictingQueue.create(MAX_HISTORICAL_CONSUMER_OFFSETS);
	}


	public String getTopicName() {
		return topicName;
	}

	public long getPartitionId() {
		return partitionId;
	}

	public Status getStatus() {
		return status;
	}

	public EvictingQueue<KafkaCommittedOffsetMetadata> getCommittedOffsets() {
		return this.committedOffsets;
	}

	private Optional<KafkaCommittedOffsetMetadata> getMaxLagOffset() {
		return committedOffsets.stream().max(committedOffsetLagCompare);
	}

	private Optional<KafkaCommittedOffsetMetadata> getMinLagOffset() {
		return committedOffsets.stream().min(committedOffsetLagCompare);
	}

	private Optional<KafkaCommittedOffsetMetadata> getMaxTimestampOffset() {
		return committedOffsets.stream().max(committedOffsetTimestampCompare);
	}

	private Optional<KafkaCommittedOffsetMetadata> getMinTimestampOffset() {
		return committedOffsets.stream().min(committedOffsetTimestampCompare);
	}

	private Stream<KafkaCommittedOffsetMetadata> getOffsetsSortedByTimestamp() {
		return committedOffsets.stream().sorted(committedOffsetTimestampCompare);
	}

	private boolean hasOffsets() {
		return (committedOffsets.size() > 0 );
	}

	/* Check if all offsets have a lag == -1, meaning we likely could not calculate lag due to lack of logEndOffset */
	private boolean hasOnlyOffsetsWithNegativeLag() {

		long committedOffsetSize = committedOffsets.size();
		long committedOffsetSizeWithNegativeLag = committedOffsets.stream().filter(o -> o.getLag() == -1).count();

		return (committedOffsetSize == committedOffsetSizeWithNegativeLag);
	}

	/* Check if there is at least one offset with no lag */
	private boolean hasOffsetWithNoLag() {
		return (committedOffsets.stream().filter(o -> o.getLag() == 0).findFirst().isPresent());
	}

	/* Check if offsets have not been committed in a while */
	/* Will return false if there are no offsets */
	private boolean areOffsetsTimely() {

		if (!hasOffsets()) { return false; }

		long maxOffsetTimestamp = getMaxLagOffset().get().getTimestamp();
		long minOffsetTimestamp = getMinLagOffset().get().getTimestamp();
		long diffNowAndMaxCommitted = System.currentTimeMillis() - maxOffsetTimestamp;

		return (diffNowAndMaxCommitted < (maxOffsetTimestamp - minOffsetTimestamp));
	}

	/* Check if the minimum offset in the queue is smaller than the first (oldest) offset */
	/* Returns false if there are no offsets in the queue */
	private boolean didConsumerOffsetsRewind() {

		if (!hasOffsets()) { return false; }

		long minLag = getMinLagOffset().get().getLag();
		long minTimestampLag = getOffsetsSortedByTimestamp().findFirst().get().getLag();

		return (minLag < minTimestampLag);
	}

	/* Check if consumer offsets do not change, and the lag is non-zero */
	/* Returns false if there are no offsets in the queue */
	private boolean isTopicPartitionStalled() {

		/* Returns false if there are no offsets in the queue */
		if (!hasOffsets()) { return false; }

		long minTimestampOffset = getMinTimestampOffset().get().getOffset();
		long maxTimestampOffset = getMaxTimestampOffset().get().getOffset();

		return ((minTimestampOffset == maxTimestampOffset) && (minTimestampOffset != 0));
	}

	/* Check if consumer offsets are changing, but lag is increasing over every commit */
	private boolean isConsumerLagGrowing() {

		/* Returns false if there are no offsets in the queue */
		if (!hasOffsets()) { return false; }

		boolean lagWentDown = false;

		KafkaCommittedOffsetMetadata[] offsetArray = getOffsetsSortedByTimestamp().toArray(KafkaCommittedOffsetMetadata[]::new);
		long previousLag = offsetArray[0].getLag();

		for (KafkaCommittedOffsetMetadata committedOffset : offsetArray) {

			long iteratingLag = committedOffset.getLag();

			// This is a Rule 3 shortcut
			if (iteratingLag == 0) {
				lagWentDown = true;
				break;
			}

			// Shortcut because lag went down
			if (iteratingLag < previousLag) {
				lagWentDown = true;
				break;
			}

			// Lag went up this iteration, so continue to the next one and check again
			previousLag = iteratingLag;
		}

		return lagWentDown;
	}

	/* Evaluate a consumer's topic-partition based on the following rules:
	 * Rule 0:  If there are no committed offsets, then there is nothing to calculate and the period is OK.
	 * Rule 1:  If the difference between now and the last offset timestamp is greater than the difference between the last and first offset timestamps, the
	 *          consumer has stopped committing offsets for that partition (error)
	 * Rule 2:  If the consumer offset decreases from one interval to the next the partition is marked as a rewind (error)
	 * Rule 3:  If over the stored period, the lag is ever zero for the partition, the period is OK
	 * Rule 4:  If the consumer offset does not change, and the lag is non-zero, it's an error (partition is stalled)
	 * Rule 5:  If the consumer offsets are moving, but the lag is consistently increasing, it's a warning (consumer is slow)
	 */
	private void calcStatus() {

		// Rule 0: If the lag is -1, this is a special value that means there are no broker offsets yet.
		// Consider it good (will get caught up in the next refresh of topics)
		if (hasOnlyOffsetsWithNegativeLag()) {
			status = Status.OK;
			return;
		}

		// Rule 1: Offsets have not been committed in a while and lag is not zero
		if (!areOffsetsTimely() && currentLag > 0) {
			status = Status.STOP;
			return;
		}

		// Rule 2: Did the consumer offsets rewind at any point?
		if (didConsumerOffsetsRewind()) {
			status = Status.REWIND;
			return;
		}

		// Rule 3: Is there ever an offset with no lag?
		if (hasOffsetWithNoLag()) {
			status = Status.OK;
			return;
		}

		// Rule 4: Is there no change in lag when lag is non-zero?
		if (isTopicPartitionStalled()) {
			status = Status.STALL;
			return;
		}

		// Rule 5: Is the consumer not keeping up?
		if (isConsumerLagGrowing()) {
			status = Status.WARN;
			return;
		}

		status = Status.OK;
	}

	public void addCommittedOffset(KafkaCommittedOffsetMetadata committedOffsetMetadata) {
		this.committedOffsets.add(committedOffsetMetadata);
		this.currentLag = getMaxTimestampOffset().get().getLag();
		calcStatus();
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KafkaTopicPartition that = (KafkaTopicPartition) o;

		if (getPartitionId() != that.getPartitionId()) return false;
		return getTopicName().equals(that.getTopicName());
	}

	@Override
	public int hashCode() {
		int result = getTopicName().hashCode();
		result = 31 * result + (int) (getPartitionId() ^ (getPartitionId() >>> 32));
		return result;
	}
}
