package eu.ideata.streaming.spark16

import java.time.{Instant, LocalDateTime}
import java.util.Properties

import com.sksamuel.avro4s.RecordFormat
import eu.ideata.streaming.core.{UserCategoryUpdateWrapper, UserInfoWrapper, UserInfoWithCategoryWrapper}
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object EnrichStreams {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafka-streaming-test").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val appConf = Config.getConfig(args)

    val kafkaParams = Map("metadata.broker.list" -> appConf.kafkaServerUrl,
      "schema.registry.url" -> appConf.schemaRegistryUrl,
      "zookeeper.connect" -> appConf.zookeeperUrl,
      "group.id" -> "kafka-spark_1_6-enrich-streaming"
    )

    val props = new Properties()

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConf.kafkaServerUrl)
    props.put("schema.registry.url", appConf.schemaRegistryUrl)

    def updateStateForUsers(batchTime: Time, key: String, value: Option[(Option[UserInfoWrapper], Option[UserCategoryUpdateWrapper])], state: State[String]): Option[UserInfoWithCategoryWrapper] = {
      value.flatMap{ case(ui, uu) => {
        uu.foreach(u => state.update(u.category))

        val category = state.getOption().getOrElse("")

        ui.map{ case UserInfoWrapper(userId, timestamp, booleanFlag, subCategory, someValue, intValue) =>  UserInfoWithCategoryWrapper(userId, category, timestamp, booleanFlag, subCategory, someValue, intValue, Instant.now().toEpochMilli, "spark_16")}

      }}
    }

    val userInfoStream: InputDStream[(Object, Object)] = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(appConf.userInfoTopic))

    val userUpdateStream: InputDStream[(Object, Object)] = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(appConf.userCategoryUpdateTopic))

    //We have to consume all the data here, user state should be an kafka connect compacted topic
    //val initalUserState = KafkaUtils.createRDD[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(userCategoryUpdateTopic)) //something


    val kafkaProps = ssc.sparkContext.broadcast(props)

    val targetTopic = ssc.sparkContext.broadcast(appConf.kafkaTargetTopic)

    val userUpdateStateSpec = StateSpec.function(updateStateForUsers _)
      .initialState(ssc.sparkContext.emptyRDD[(String,String)])
      .numPartitions(4)

    val s1: DStream[(String, UserInfoWrapper)] = userInfoStream.mapPartitions(itr => {
      val format = RecordFormat[UserInfoWrapper]

      itr.map { case (k,v) => {
        val u = format.from(v.asInstanceOf[GenericRecord])
        u.userId -> u
      }}
    })


    val s2: DStream[(String, UserCategoryUpdateWrapper)] = userUpdateStream.mapPartitions(itr => {
      val format = RecordFormat[UserCategoryUpdateWrapper]

      itr.map{ case (k,v) => {
        val u = format.from(v.asInstanceOf[GenericRecord])
        u.userId -> u
      }}
    })


    val stateStream = s1.fullOuterJoin(s2).mapWithState(userUpdateStateSpec)

    stateStream.stateSnapshots()

    stateStream.foreachRDD( rdd => {
      rdd.foreachPartition(partition => {

        val formater = RecordFormat[UserInfoWithCategoryWrapper]

        val topic = targetTopic.value
        val kafkaParams = kafkaProps.value

        val producer = new KafkaProducer[Object, Object](kafkaParams)

        partition.foreach(record => {
          val message = new ProducerRecord[Object, Object](topic, record.userId, formater.to(record))
          producer.send(message)
        })
      })
    }
    )

    ssc.checkpoint("checkpoint/")


    ssc.start()

    ssc.awaitTermination()

  }
}




