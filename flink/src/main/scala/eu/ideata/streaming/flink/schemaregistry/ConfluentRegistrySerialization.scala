package eu.ideata.streaming.flink.schemaregistry

import KafkaKV
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

case class ConfluentRegistrySerialization(topic: String, schemaRegistryUrl: String) extends KeyedSerializationSchema[KafkaKV]{

  @transient lazy val valueSerializer = {
    val serializer = new KafkaAvroSerializer()
    serializer.configure( Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava, false)
    serializer
  }

  @transient lazy val keySerializer = {
    val serializer = new KafkaAvroSerializer()
    serializer.configure( Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava, true)
    serializer
  }

  override def serializeKey(keyedMessages: KafkaKV): Array[Byte] =
    keySerializer.serialize(topic, keyedMessages.key)

  override def getTargetTopic(element: KafkaKV): String = topic

  override def serializeValue(keyedMessages: KafkaKV): Array[Byte] =
    valueSerializer.serialize(topic, keyedMessages.value)
}
