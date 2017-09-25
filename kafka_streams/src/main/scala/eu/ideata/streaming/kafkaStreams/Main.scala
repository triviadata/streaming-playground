package eu.ideata.streaming.kafkaStreams

import java.time.Instant
import java.util.Properties

import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo, UserInfoWithCategory}
import eu.ideata.streaming.kafkaStreams.utils.{Config, SpecificAvroSerde}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable, ValueJoiner}

import scala.collection.JavaConverters._

class UserInfoCategoryJoiner(val streamingSource: String) extends ValueJoiner[UserInfo, UserCategoryUpdate, UserInfoWithCategory] {
  override def apply(value1: UserInfo, value2: UserCategoryUpdate): UserInfoWithCategory = {
      val category = Option(value2).map(_.getCategory).getOrElse("")
      new UserInfoWithCategory(value1.getUserId, category, value1.getTimestamp, value1.getBooleanFlag, value1.getSubCategory, value1.getSomeValue, value1.getIntValue, Instant.now().getEpochSecond, streamingSource)
    }
  }

object Pipe {

  val builder = new KStreamBuilder

  def main(args: Array[String]): Unit = {

    val conf = Config.getConfig(args)

    def config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrich-streams")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, conf.kafkaServerUrl)
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, conf.schemaRegistryUrl)

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

    val userInfoStream: KStream[String, UserInfo] = builder.stream(keySerde, userInfoSerde, conf.userInfoTopic)

    val userCategoryTable: KTable[String, UserCategoryUpdate] = builder.table(keySerde, categoryUpdateSerde, conf.userCategoryUpdateTopic, "user_category_compacted")

    val userInfoCategoryJoiner = new UserInfoCategoryJoiner(conf.streamingSource)

    val joined: KStream[String, UserInfoWithCategory] = userInfoStream
      .leftJoin(userCategoryTable, userInfoCategoryJoiner)

    joined.to(keySerde,sinkSerde, conf.kafkaTargetTopic)

    val stream = new KafkaStreams(builder, config)

    stream.start()
  }
}