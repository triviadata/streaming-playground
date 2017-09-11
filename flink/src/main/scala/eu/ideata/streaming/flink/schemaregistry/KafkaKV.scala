package eu.ideata.streaming.flink.schemaregistry

import org.apache.avro.generic.GenericRecord

case class KafkaKV(key: String, value: GenericRecord)
