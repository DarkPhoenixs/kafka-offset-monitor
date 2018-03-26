package com.quantifind.kafka.core

import com.morningstar.kafka.KafkaConsumerGroup
import com.quantifind.kafka.OffsetGetter
import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.utils.ZkUtilsWrapper

import com.twitter.util.Time
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.apache.zookeeper.data.Stat
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.common.TopicPartition

import scala.collection._
import scala.util.control.NonFatal

/**
 * a nicer version of kafka's ConsumerOffsetChecker tool
 * User: pierre
 * Date: 1/22/14
 */
class ZKOffsetGetter(theZkUtils: ZkUtilsWrapper) extends OffsetGetter {

  override val zkUtils = theZkUtils

  override def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {
    try {
      val (offset, stat: Stat) = zkUtils.readData(s"${ZkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
      val (owner, _) = zkUtils.readDataMaybeNull(s"${ZkUtils.ConsumersPath}/$group/owners/$topic/$pid")

      zkUtils.getLeaderForPartition(topic, pid) match {
        case Some(bid) =>
          val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt map {
            consumer =>
              val topicAndPartition = TopicAndPartition(topic, pid)
              val request =
                OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head

              OffsetInfo(group = group,
                topic = topic,
                partition = pid,
                offset = offset.toLong,
                logSize = logSize,
                owner = owner,
                creation = Time.fromMilliseconds(stat.getCtime),
                modified = Time.fromMilliseconds(stat.getMtime))
          }
        case None =>
          error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case NonFatal(t) =>
        error(s"Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  override def getGroups: Seq[String] = {
    try {
      zkUtils.getChildren(ZkUtils.ConsumersPath)
    } catch {
      case NonFatal(t) =>
        error(s"could not get groups because of ${t.getMessage}", t)
        Seq()
    }
  }

  override def getActiveGroups: Seq[String] = getGroups

  override def getTopicList(group: String): List[String] = {
    try {
      zkUtils.getChildren(s"${ZkUtils.ConsumersPath}/$group/offsets").toList
    } catch {
      case _: ZkNoNodeException => List()
    }
  }

  /**
   * Returns a map of topics -> list of consumers, including non-active
   */
  override def getTopicMap: Map[String, Seq[String]] = {
    try {
      zkUtils.getChildren(ZkUtils.ConsumersPath).flatMap {
        group => {
          getTopicList(group).map(topic => topic -> group)
        }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    try {
      zkUtils.getChildren(ZkUtils.ConsumersPath).flatMap {
        group =>
          try {
            zkUtils.getConsumersPerTopic(group, true).keySet.map {
              key =>
                key -> group
            }
          } catch {
            case NonFatal(t) =>
              error(s"could not get consumers for group $group", t)
              Seq()
          }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }

  override def getTopicsAndLogEndOffsets: Seq[TopicLogEndOffsetInfo] = ???

  override def getConsumerGroupStatus: String = ???
}
