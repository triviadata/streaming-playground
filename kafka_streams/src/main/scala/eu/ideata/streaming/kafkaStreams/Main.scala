package eu.ideata.streaming.kafkaStreams

import java.time.Instant
import java.util.Properties

import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo, UserInfoWithCategory, UserInfoWrapper}
import eu.ideata.streaming.kafkaStreams.config.Config
import eu.ideata.streaming.kafkaStreams.serialization.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable, ValueJoiner, ValueMapper}

import scala.collection.JavaConverters._
import org.apache.kafka.streams._

import scala.collection.JavaConverters.asJavaIterableConverter

class UserInfoCategoryJoiner(val streamingSource: String) extends ValueJoiner[UserInfoWrapper, UserCategoryUpdate, UserInfoWithCategory] {
  override def apply(value1: UserInfoWrapper, value2: UserCategoryUpdate): UserInfoWithCategory = {
      val category = Option(value2).map(_.getCategory).getOrElse("")
      new UserInfoWithCategory(value1.userId, category, value1.timestamp, value1.booleanFlag, value1.subCategory, value1.someValue, value1.intValue, Instant.now().toEpochMilli, streamingSource, value1.readTimeStamp)
    }
  }

object UserInfoValueMapper extends ValueMapper[UserInfo, UserInfoWrapper] {
  override def apply(value: UserInfo) = UserInfoWrapper.fromJava(value)
}

object Pipe {

  val builder = new KStreamBuilder
  val r = new scala.util.Random()

  def main(args: Array[String]): Unit = {

    val conf = Config.getConfig(args)

    def config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, conf.applicationId)
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, conf.kafkaServerUrl)
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, conf.schemaRegistryUrl)
      p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, conf.threads.toString)

      if(conf.fromBeginning) p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      p
    }

    val schemaRegistryConf = Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> conf.schemaRegistryUrl).asJava

    val keySerde = Serdes.String
    val userInfoSerde = new SpecificAvroSerde[UserInfo]
    userInfoSerde.configure(schemaRegistryConf, false)

    val categoryUpdateSerde = new SpecificAvroSerde[UserCategoryUpdate]
    categoryUpdateSerde.configure(schemaRegistryConf, false)

    val sinkSerde = new SpecificAvroSerde[UserInfoWithCategory]
    sinkSerde.configure(schemaRegistryConf, false)

    val userInfoStream: KStream[String, UserInfoWrapper] = builder.stream(keySerde, userInfoSerde, conf.userInfoTopic).mapValues(UserInfoValueMapper)

    val userCategoryTable: KTable[String, UserCategoryUpdate] = builder.table(keySerde, categoryUpdateSerde, conf.userCategoryUpdateTopic, "user_category_compacted")

    val userInfoCategoryJoiner = new UserInfoCategoryJoiner(conf.streamingSource)

    val joined: KStream[String, UserInfoWithCategory] = userInfoStream
      .leftJoin(userCategoryTable, userInfoCategoryJoiner)

    joined.to(keySerde, sinkSerde, conf.kafkaTargetTopic)

    val stream = new KafkaStreams(builder, config)

    stream.start()
  }
}