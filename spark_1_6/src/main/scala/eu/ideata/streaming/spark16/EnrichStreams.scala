package eu.ideata.streaming.spark16

import java.time.Instant
import java.util.Properties

import eu.ideata.streaming.core._
import io.confluent.kafka.serializers.{KafkaAvroDecoder, KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import org.apache.avro.specific.SpecificData
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils


object EnrichStreams {

  def main(args: Array[String]): Unit = {
    val (sparkConf, appConf) = {
      val conf = new SparkConf().setAppName("kafka-streaming-test")
      val appConf = EnrichStreamsConfig.getConfig(args)
      if(appConf.local) (conf.setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true"), appConf) else (conf, appConf)
    }

    val ssc = new StreamingContext(sparkConf, Seconds(appConf.seconds))

    val sparkGroup: Broadcast[String] = ssc.sparkContext.broadcast(appConf.sparkGroup)

    val kafkaParams: Map[String, String] = {
      val params: Map[String,String] = Map(
        "metadata.broker.list" -> appConf.kafkaServerUrl,
        "schema.registry.url" -> appConf.schemaRegistryUrl,
        "zookeeper.connect" -> appConf.zookeeperUrl,
        "group.id" -> appConf.groupId
      )

      if (appConf.fromBeginning) params ++ Map(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest") else params
    }

    val producerProperties = {
      val producerProperties = new Properties()
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
      producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConf.kafkaServerUrl)
      producerProperties.put("schema.registry.url", appConf.schemaRegistryUrl)
      producerProperties
    }

    def updateStateForUsers(batchTime: Time, key: String, value: Option[(Option[UserInfoWrapper], Option[UserCategoryUpdateWrapper])], state: State[String]): Option[UserInfoWithCategoryWrapper] = {
      value.flatMap{ case(ui, uu) => {
        uu.foreach(u => state.update(u.category))

        val category = state.getOption().getOrElse("")

        ui.map{ case UserInfoWrapper(userId, timestamp, booleanFlag, subCategory, someValue, intValue) =>  UserInfoWithCategoryWrapper(userId, category, timestamp, booleanFlag, subCategory, someValue, intValue, Instant.now().toEpochMilli, sparkGroup.value)}

      }}
    }

    val userInfoStream: InputDStream[(Object, Object)] = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(appConf.userInfoTopic))

    val userUpdateStream: InputDStream[(Object, Object)] = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(appConf.userCategoryUpdateTopic))

    //We have to consume all the data here, user state should be an kafka connect compacted topic
    //val initalUserState = KafkaUtils.createRDD[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(userCategoryUpdateTopic)) //something


    val kafkaProps = ssc.sparkContext.broadcast(producerProperties)

    val targetTopic = ssc.sparkContext.broadcast(appConf.kafkaTargetTopic)

    val userUpdateStateSpec = StateSpec.function(updateStateForUsers _)
      .initialState(ssc.sparkContext.emptyRDD[(String,String)])
      .numPartitions(appConf.statePartitionCount)

    val s1: DStream[(String, UserInfoWrapper)] = userInfoStream.mapPartitions(itr => {
      lazy val spec = new SpecificData()

      itr.map { case (k,v) => {
        val u = UserInfoWrapper.fromJava(spec.deepCopy(UserInfo.getClassSchema, v).asInstanceOf[UserInfo])
        u.userId -> u
      }}
    })

    val s2: DStream[(String, UserCategoryUpdateWrapper)] = userUpdateStream.mapPartitions(itr => {
      lazy val spec = new SpecificData()

      itr.map{ case (k,v) => {
        val u = UserCategoryUpdateWrapper.fromJava(spec.deepCopy(UserCategoryUpdate.getClassSchema, v).asInstanceOf[UserCategoryUpdate])
        u.userId -> u
      }}
    })

    val stateStream = s1.fullOuterJoin(s2).mapWithState(userUpdateStateSpec)

    stateStream.stateSnapshots()

    stateStream.foreachRDD( rdd => {
      rdd.foreachPartition(partition => {

        val topic = targetTopic.value
        val kafkaParams = kafkaProps.value

        val producer = new KafkaProducer[Object, Object](kafkaParams)

        partition.foreach(record => {
            val message = new ProducerRecord[Object, Object](topic, record.userId, record.asJava)
            producer.send(message)
          })
        })
      }
    )

    ssc.checkpoint(appConf.checkpointDir)

    ssc.start()

    ssc.awaitTermination()

  }
}




