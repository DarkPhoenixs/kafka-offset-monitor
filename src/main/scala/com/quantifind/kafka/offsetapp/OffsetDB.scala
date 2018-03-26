package com.quantifind.kafka.offsetapp

import scala.slick.driver.SQLiteDriver.simple._
import scala.slick.jdbc.JdbcBackend
import scala.slick.jdbc.meta.MTable

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.OffsetDB.{DbOffsetInfo, OffsetHistory, OffsetPoints}
import com.twitter.util.Time

/**
  * Store and retrieve offsets in a SQLite file
	* User: andrews, rcasey212
	* Date: 1/27/14
	*/
class OffsetDB(dbfile: String) {

	var sqliteDbLock: AnyRef = new Object()

	val database = Database.forURL(s"jdbc:sqlite:$dbfile.db",
		driver = "org.sqlite.JDBC")

	implicit val twitterTimeMap = MappedColumnType.base[Time, Long](
		{
			time => time.inMillis
		}, {
			millis => Time.fromMilliseconds(millis)
		}
	)

	class Offset(tag: Tag) extends Table[DbOffsetInfo](tag, "OFFSETS") {
		def id = column[Int]("id", O.PrimaryKey, O.AutoInc)

		val group = column[String]("group")
		val topic = column[String]("topic")
		val partition = column[Int]("partition")
		val offset = column[Long]("offset")
		val logSize = column[Long]("log_size")
		val owner = column[Option[String]]("owner")
		val timestamp = column[Long]("timestamp")
		val creation = column[Time]("creation")
		val modified = column[Time]("modified")


		def * = (id.?, group, topic, partition, offset, logSize, owner, timestamp, creation, modified).shaped <> (DbOffsetInfo.parse, DbOffsetInfo.unparse)

		def forHistory = (timestamp, partition, owner, offset, logSize) <> (OffsetPoints.tupled, OffsetPoints.unapply)

		def idx = index("idx_search", (group, topic))

		def tidx = index("idx_time", (timestamp))

		def uidx = index("idx_unique", (group, topic, partition, timestamp), unique = true)
	}

	val offsets = TableQuery[Offset]

	def insert(timestamp: Long, info: OffsetInfo) {
		sqliteDbLock.synchronized {
			database.withSession {
				implicit s =>
					offsets += DbOffsetInfo(timestamp = timestamp, offset = info)
			}
		}
	}

	def insertAll(info: IndexedSeq[OffsetInfo]) {
		val now = System.currentTimeMillis
		sqliteDbLock.synchronized {
			database.withTransaction {
				implicit s =>
					offsets ++= info.map(i => DbOffsetInfo(timestamp = now, offset = i))
			}
		}
	}

	def emptyOld(since: Long) {
		sqliteDbLock.synchronized {
			database.withSession {
				implicit s =>
					offsets.filter(_.timestamp < since).delete
			}
		}
	}

	def offsetHistory(group: String, topic: String): OffsetHistory = database.withSession {
		sqliteDbLock.synchronized {
			implicit s =>
				val o = offsets
						.filter(off => off.group === group && off.topic === topic)
						.sortBy(_.timestamp)
						.map(_.forHistory)
						.list(implicitly[JdbcBackend#Session])
				OffsetHistory(group, topic, o)
		}
	}

	def maybeCreate() {
		sqliteDbLock.synchronized {
			database.withSession {
				implicit s =>
					if (MTable.getTables("OFFSETS").list(implicitly[JdbcBackend#Session]).isEmpty) {
						offsets.ddl.create
					}
			}
		}
	}
}

object OffsetDB {

	case class OffsetPoints(timestamp: Long, partition: Int, owner: Option[String], offset: Long, logSize: Long)

	case class OffsetHistory(group: String, topic: String, offsets: Seq[OffsetPoints])

	case class DbOffsetInfo(id: Option[Int] = None, timestamp: Long, offset: OffsetInfo)

	object DbOffsetInfo {
		def parse(in: (Option[Int], String, String, Int, Long, Long, Option[String], Long, Time, Time)): DbOffsetInfo = {
			val (id, group, topic, partition, offset, logSize, owner, timestamp, creation, modified) = in
			DbOffsetInfo(id, timestamp, OffsetInfo(group, topic, partition, offset, logSize, owner, creation, modified))
		}

		def unparse(in: DbOffsetInfo): Option[(Option[Int], String, String, Int, Long, Long, Option[String], Long, Time, Time)] = Some(
			in.id,
			in.offset.group,
			in.offset.topic,
			in.offset.partition,
			in.offset.offset,
			in.offset.logSize,
			in.offset.owner,
			in.timestamp,
			in.offset.creation,
			in.offset.modified
		)
	}
}
