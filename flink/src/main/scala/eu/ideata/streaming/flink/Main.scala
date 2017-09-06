package eu.ideata.streaming.flink

//sv3nd.github.io/how-to-integrate-flink-with-confluents-schema-registry.html#how-to-integrate-flink-with-confluents-schema-registry



import java.util.Properties

import com.sksamuel.avro4s.{FromRecord, RecordFormat, SchemaFor, ToRecord}
import eu.ideata.streaming.core.UserInfo
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.windowing.time.Time
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object Main {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val consumer = new FlinkKafkaConsumer010("user_info", new ConfluentRegistryDeserialization("user_info",  "http://localhost:8081"), properties)

    env
      .addSource(consumer)
      .print()

    env.execute("Simple consumption")
  }
}


case class ConfluentRegistryDeserialization(topic: String, schemaRegistryUrl: String) extends KeyedDeserializationSchema[(String, UserInfo)] {

  @transient lazy val format = RecordFormat[UserInfo]

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

  override def isEndOfStream(nextElement: (String, UserInfo)): Boolean = false

  override def deserialize(messageKey: Array[Byte], message: Array[Byte],
                           topic: String, partition: Int, offset: Long): (String, UserInfo) = {

    val key = keyDeserializer.deserialize(topic, messageKey).asInstanceOf[String]
    val value = format.from(valueDeserializer.deserialize(topic, message).asInstanceOf[GenericRecord])

    (key, value)
  }

  override def getProducedType: TypeInformation[(String, UserInfo)] =
    TypeExtractor.getForClass(classOf[(String, UserInfo)])
}




