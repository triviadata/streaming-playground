package eu.ideata.streaming.kafkaStreams

import java.time.Instant
import java.util.Properties

import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo, UserInfoWithCategory}
import eu.ideata.streaming.kafkaStreams.utils.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable, ValueJoiner}

import scala.collection.JavaConverters._

object UserInfoCategoryJoiner extends ValueJoiner[UserInfo, UserCategoryUpdate, UserInfoWithCategory] {
  override def apply(value1: UserInfo, value2: UserCategoryUpdate): UserInfoWithCategory = {
      if(value2 != null){
        new UserInfoWithCategory(value1.getUserId, value2.getCategory, value1.getTimestamp, value1.getBooleanFlag, value1.getSubCategory, value1.getSomeValue, value1.getIntValue, Instant.now().getEpochSecond, "kafka-streams")
      } else {
        new UserInfoWithCategory(value1.getUserId, "empty", value1.getTimestamp, value1.getBooleanFlag, value1.getSubCategory, value1.getSomeValue, value1.getIntValue, Instant.now().getEpochSecond, "kafka-streams")
      }
    }
  }

object Pipe {

  val builder = new KStreamBuilder

  def main(args: Array[String]): Unit = {
    def config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "enrich-streams")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092")
      p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p
    }

    val keySerde = Serdes.String
    val userInfoSerde = new SpecificAvroSerde[UserInfo]
    userInfoSerde.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081").asJava, false)

    val categoryUpdateSerde = new SpecificAvroSerde[UserCategoryUpdate]
    categoryUpdateSerde.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081").asJava, false)

    val sinkSerde = new SpecificAvroSerde[UserInfoWithCategory]
    sinkSerde.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081").asJava, false)

    val userInfoStream: KStream[String, UserInfo] = builder.stream(keySerde, userInfoSerde, "user_info")

    val userCategoryTable: KTable[String, UserCategoryUpdate] = builder.table(keySerde, categoryUpdateSerde, "user_update", "user_category_compacted")

    val joined: KStream[String, UserInfoWithCategory] = userInfoStream
      .leftJoin(userCategoryTable, UserInfoCategoryJoiner)

    joined.to(keySerde,sinkSerde, "enriched_user" )

    val stream = new KafkaStreams(builder, config)

    stream.start()
  }
}