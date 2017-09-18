package eu.ideata.streaming.flink.schemaregistry

import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import scala.collection.JavaConverters._

case class ConfluentRegistryDeserialization(topic: String, schemaRegistryUrl: String) extends KeyedDeserializationSchema[(String, GenericRecord)] {


  @transient lazy val valueDeserializer = {
    val deserializer = new KafkaAvroDeserializer()
    deserializer.configure( Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava, false)

    deserializer
  }

  @transient lazy val keyDeserializer = {
    val deserializer = new KafkaAvroDeserializer()
    deserializer.configure( Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava,  true)
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
