package eu.ideata.streaming.flink.schemaregistry

import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo}
import eu.ideata.streaming.flink.serialization.SpecificAvroDeserializer
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.specific.SpecificData
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.util.serialization.{AbstractDeserializationSchema, KeyedDeserializationSchema}

import scala.collection.JavaConverters._

case class ConfluentRegistryDeserializationUserInfo(topic: String, schemaRegistryUrl: String) extends KeyedDeserializationSchema[UserInfo] {


  @transient lazy val valueDeserializer = {
    val deserializer = new SpecificAvroDeserializer[UserInfo]()
    deserializer.configure( Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava, false)

    deserializer
  }

  override def isEndOfStream(nextElement:  UserInfo): Boolean = false


  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): UserInfo = valueDeserializer.deserialize(topic, message)

  override def getProducedType: TypeInformation[UserInfo] =
    TypeExtractor.getForClass(classOf[UserInfo])
}


case class ConfluentRegistryDeserializationUserCategory(topic: String, schemaRegistryUrl: String) extends KeyedDeserializationSchema[UserCategoryUpdate] {


  @transient lazy val valueDeserializer = {
    val deserializer = new SpecificAvroDeserializer[UserCategoryUpdate]()
    deserializer.configure( Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistryUrl).asJava, false)

    deserializer
  }

  override def isEndOfStream(nextElement:  UserCategoryUpdate): Boolean = false

  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): UserCategoryUpdate = valueDeserializer.deserialize(topic, message)

  override def getProducedType: TypeInformation[UserCategoryUpdate] =
    TypeExtractor.getForClass(classOf[UserCategoryUpdate])
}