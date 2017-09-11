package eu.ideata.streaming.flink

//sv3nd.github.io/how-to-integrate-flink-with-confluents-schema-registry.html#how-to-integrate-flink-with-confluents-schema-registry



import java.time.Instant
import java.util.Properties

import eu.ideata.streaming.core._
import eu.ideata.streaming.flink.schemaregistry.{ConfluentRegistryDeserialization, ConfluentRegistrySerialization, KafkaKV}
import org.apache.flink.streaming.api.scala._

import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificData
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
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

object ToUserInfo extends RichMapFunction[(String, GenericRecord), UserInfoWrapper] {

  @transient lazy val spec = new SpecificData()

  def map(in: (String, GenericRecord)): UserInfoWrapper = {
    val data = spec.deepCopy(UserInfo.getClassSchema, in._2).asInstanceOf[UserInfo]
    UserInfoWrapper.fromJava(data)
  }

}

object ToUserCategoryUpdate extends RichMapFunction[(String, GenericRecord), UserCategoryUpdateWrapper] {
  @transient lazy val spec = new SpecificData()

  def map(in: (String, GenericRecord)): UserCategoryUpdateWrapper ={
    val data = spec.deepCopy(UserCategoryUpdate.getClassSchema, in._2).asInstanceOf[UserCategoryUpdate]
    UserCategoryUpdateWrapper.fromJava(data)
  }
}

object ToUserWithCategory extends RichMapFunction[UserInfoWithCategoryWrapper, KafkaKV] {

  def map(in: UserInfoWithCategoryWrapper): KafkaKV = KafkaKV(in.userId, in.asJava)

}

object StateMap extends RichCoFlatMapFunction[UserInfoWrapper, UserCategoryUpdateWrapper, UserInfoWithCategoryWrapper]{

  private var userCategoryState: MapState[String, String] = _

  override def open(parameters: Configuration) = {
    userCategoryState =  getRuntimeContext.getMapState[String, String](
      new MapStateDescriptor[String,String]("users", classOf[String], classOf[String])
    )
  }

  override def flatMap2(value: UserCategoryUpdateWrapper, out: Collector[UserInfoWithCategoryWrapper]): Unit = {
    userCategoryState.put(value.userId, value.userId)
    out.close()
  }

  override def flatMap1(value: UserInfoWrapper, out: Collector[UserInfoWithCategoryWrapper]): Unit = {

    val category = if(userCategoryState.contains(value.userId)) userCategoryState.get(value.userId) else ""

    val UserInfoWrapper(userId, timestamp, booleanFlag, subCategory, someValue, intValue) = value

    val enriched = UserInfoWithCategoryWrapper(userId, category, timestamp, booleanFlag, subCategory, someValue, intValue, Instant.now().toEpochMilli, "flink")

    out.collect(enriched)
    out.close()
  }
}


