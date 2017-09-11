package eu.ideata.streaming.flink

//sv3nd.github.io/how-to-integrate-flink-with-confluents-schema-registry.html#how-to-integrate-flink-with-confluents-schema-registry



import java.time.Instant
import java.util.Properties

import com.sksamuel.avro4s.{FromRecord, RecordFormat, SchemaFor, ToRecord}
import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo, UserInfoWithCategory}
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.windowing.time.Time
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.optimizer.operators.MapDescriptor
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.co.{RichCoFlatMapFunction, RichCoMapFunction}
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, KeyedSerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector

object Main {

  def main(args: Array[String]): Unit = {

    val sourcePropertis = new Properties()

    sourcePropertis.setProperty("bootstrap.servers", "localhost:9092")
    sourcePropertis.setProperty("group.id", "test")

    val sinkProperties = new Properties()

    sinkProperties.setProperty("bootstrap.servers", "localhost:9092")
    sinkProperties.setProperty("group.id", "test")


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val userInfo = new FlinkKafkaConsumer010("user_info", new ConfluentRegistryDeserialization("user_info",  "http://localhost:8081"), sourcePropertis)

    val userCategory = new FlinkKafkaConsumer010("user_update", new ConfluentRegistryDeserialization("user_update",  "http://localhost:8081"), sourcePropertis)

    val userInfoStream = env
      .addSource(userInfo)
      .map(ToUserInfo)

    val userCategoryStream = env
      .addSource(userCategory)
      .map(ToUserCategoryUpdate)

   val enriched: DataStream[KafkaKV] =  userInfoStream
      .connect(userCategoryStream)
      .keyBy("userId", "userId")
      .flatMap(StateMap)
      .map(ToUserWithCategory)

    val sinkSerializer: KeyedSerializationSchema[KafkaKV] = new ConfluentRegistrySerialization("enriched_user", "http://localhost:8081")

    val sink = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
      enriched.javaStream,
      "enriched_user",
      sinkSerializer, sinkProperties)

    sink.setFlushOnCheckpoint(true)

    env.enableCheckpointing(1000)
    env.setStateBackend(new FsStateBackend("file:///Users/mbarak/projects/ideata/streaming-playground/checkpoints"))
    env.execute("Simple consumption")
  }
}

object ToUserInfo extends RichMapFunction[(String, GenericRecord), UserInfo] {
  @transient lazy val format = RecordFormat[UserInfo]

  def map(in: (String, GenericRecord)): UserInfo = format.from(in._2)

}

object ToUserCategoryUpdate extends RichMapFunction[(String, GenericRecord), UserCategoryUpdate] {
  @transient lazy val format = RecordFormat[UserCategoryUpdate]

  def map(in: (String, GenericRecord)): UserCategoryUpdate = format.from(in._2)
}

object ToUserWithCategory extends RichMapFunction[UserInfoWithCategory, KafkaKV] {
  @transient lazy val format = RecordFormat[UserInfoWithCategory]

  def map(in: UserInfoWithCategory): KafkaKV = KafkaKV(in.userId, format.to(in))

}

object StateMap extends RichCoFlatMapFunction[UserInfo, UserCategoryUpdate, UserInfoWithCategory]{

  private var userCategoryState: MapState[String, String] = _

  override def open(parameters: Configuration) = {
    userCategoryState =  getRuntimeContext.getMapState[String, String](
      new MapStateDescriptor[String,String]("users", classOf[String], classOf[String])
    )
  }

  override def flatMap2(value: UserCategoryUpdate, out: Collector[UserInfoWithCategory]): Unit = {
    userCategoryState.put(value.userId, value.userId)
    out.close()
  }

  override def flatMap1(value: UserInfo, out: Collector[UserInfoWithCategory]): Unit = {

    val category = if(userCategoryState.contains(value.userId)) userCategoryState.get(value.userId) else ""

    val UserInfo(userId, timestamp, booleanFlag, subCategory, someValue, intValue) = value

    val enriched = UserInfoWithCategory(userId, category, timestamp, booleanFlag, subCategory, someValue, intValue, Instant.now().toEpochMilli)

    out.collect(enriched)
    out.close()
  }
}

case class ConfluentRegistryDeserialization(topic: String, schemaRegistryUrl: String) extends KeyedDeserializationSchema[(String, GenericRecord)] {

  // Flink needs the serializer to be serializable => this "@transient lazy val" does the trick
  @transient lazy val valueDeserializer = {
    val deserializer = new KafkaAvroDeserializer()
    deserializer.configure(
      // other schema-registry configuration parameters can be passed, see the configure() code
      // for details (among other things, schema cache size)
      Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava,
      false)
    deserializer
  }

  @transient lazy val keyDeserializer = {
    val deserializer = new KafkaAvroDeserializer()
    deserializer.configure(
      Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava,
      true)
    deserializer
  }

  override def isEndOfStream(nextElement: (String, GenericRecord)): Boolean = false

  override def deserialize(messageKey: Array[Byte], message: Array[Byte],
                           topic: String, partition: Int, offset: Long): (String, GenericRecord) = {

    val key = keyDeserializer.deserialize(topic, messageKey).asInstanceOf[String]
    val value = valueDeserializer.deserialize(topic, message).asInstanceOf[GenericRecord]

    (key, value)
  }

  override def getProducedType: TypeInformation[(String, GenericRecord)] =
    TypeExtractor.getForClass(classOf[(String, GenericRecord)])
}



case class KafkaKV(key: String, value: GenericRecord)

case class ConfluentRegistrySerialization(topic: String, schemaRegistryUrl: String) extends KeyedSerializationSchema[KafkaKV]{

  @transient lazy val valueSerializer = {
    val serializer = new KafkaAvroSerializer()
    serializer.configure(
      Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava,
      false)
    serializer
  }

  @transient lazy val keySerializer = {
    val serializer = new KafkaAvroSerializer()
    serializer.configure(
      Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava,
      true)
    serializer
  }

  override def serializeKey(keyedMessages: KafkaKV): Array[Byte] =
    keySerializer.serialize(topic, keyedMessages.key)

  override def getTargetTopic(element: KafkaKV): String = topic

  override def serializeValue(keyedMessages: KafkaKV): Array[Byte] =
    valueSerializer.serialize(topic, keyedMessages.value)
}




