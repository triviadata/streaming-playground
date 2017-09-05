package eu.ideata.streaming.spark16

import java.util.Properties

import com.sksamuel.avro4s.RecordFormat
import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo, UserInfoWithCategory}
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import scopt.OptionParser

object EnrichStreams {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafka-streaming-test").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val appConf = getConfig(args)

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

    val job = new JoinStreaming(sc, ssc, kafkaParams, appConf.userInfoTopic, appConf.userCategoryUpdateTopic, props, appConf.kafkaTargetTopic)

    ssc.awaitTermination()

  }

  case class StreamingConfig(seconds: Int, kafkaServerUrl: String, schemaRegistryUrl: String, userInfoTopic: String, userCategoryUpdateTopic: String, zookeeperUrl: String, kafkaTargetTopic: String)

  def getConfig(args: Array[String]): StreamingConfig = {

    lazy val emptyConf = StreamingConfig(5, "localhost:9092", "http://localhost:8081", "user_info", "user_update", "localhost:2181", "enriched_user")

    lazy val parser = new OptionParser[StreamingConfig]("scopt") {
      opt[Int]("seconds")
        .optional()
        .action {
          case (s, conf) => conf.copy(seconds = s)
        }

      opt[String]("kafkaServerUrl")
        .optional()
        .action {
          case (s, conf) => conf.copy(kafkaServerUrl = s)
        }

      opt[String]("schemaRegistryUrl")
        .optional()
        .action {
          case (s, conf) => conf.copy(schemaRegistryUrl = s)
        }

      opt[String]("userInfoTopic")
        .optional()
        .action {
          case (s, conf) => conf.copy(userInfoTopic = s)
        }

      opt[String]("userCategoryUpdateTopic")
        .optional()
        .action {
          case (s, conf) => conf.copy(userCategoryUpdateTopic = s)
        }

      opt[String]("zookeeperUrl")
        .optional()
        .action {
          case (s, conf) => conf.copy(zookeeperUrl = s)
        }

      opt[String]("kafkaTargetTopic")
        .optional()
        .action {
          case (s, conf) => conf.copy(kafkaTargetTopic = s)
        }.text("In seconds")
    }

    parser.parse(args, emptyConf).getOrElse(emptyConf)
  }
}



class JoinStreaming(val sc: SparkContext, val ssc: StreamingContext, kafkaParams: Map[String,String], userInfoTopic: String, userCategoryUpdateTopic: String, kafkaOutputParams: Properties, kafkaTargetTopic: String) {

  val userInfo = sc.broadcast(RecordFormat[UserInfo])
  val categoryUpdateFormat = sc.broadcast(RecordFormat[UserCategoryUpdate])
  val userInfoWithCategory = sc.broadcast(RecordFormat[UserInfoWithCategory])

  def updateStateForUsers(batchTime: Time, key: String, value: Option[(Option[UserInfo], Option[UserCategoryUpdate])], state: State[String]): Option[UserInfoWithCategory] = {
    value.flatMap{ case(ui, uu) => {
      uu.foreach(u => state.update(u.category))

      val category = state.getOption().getOrElse(null)

      ui.map{ case UserInfo(userId, timestamp, booleanFlag, subCategory, someValue, intValue) =>  UserInfoWithCategory(userId, category, timestamp, booleanFlag, subCategory, someValue, intValue) }

    }}
  }

  def job = {

    val userInfoStream: InputDStream[(Object, Object)] = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(userInfoTopic))

    val userUpdateStream: InputDStream[(Object, Object)] = KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(userCategoryUpdateTopic))

    //We have to consume all the data here, user state should be an kafka connect compacted topic, but we still risk loosing some data, best approach will be just to read the whole phoenix table with the latest timestamp
    //val initalUserState = KafkaUtils.createRDD[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, Set(userCategoryUpdateTopic))


    val kafkaSink = sc.broadcast(kafkaOutputParams)

    val targetTopic = sc.broadcast(kafkaTargetTopic)

    val userUpdateStateSpec = StateSpec.function(updateStateForUsers _)

    val s1: DStream[(String, UserInfo)] = userInfoStream.map{ case (k,ui) => {
        val user = userInfo.value.from(ui.asInstanceOf[GenericRecord])
        user.userId -> user
      }
    }

    val s2: DStream[(String, UserCategoryUpdate)] = userUpdateStream.map{ case (k, ui) => {
      val user = categoryUpdateFormat.value.from(ui.asInstanceOf[GenericRecord])
      user.userId -> user
    }}


    val stateStream = s1.fullOuterJoin(s2).mapWithState(userUpdateStateSpec)


    stateStream.foreachRDD( rdd => {
      rdd.foreachPartition(partition => {

        val topic = targetTopic.value
        val kafkaParams = kafkaSink.value
        val formater = userInfoWithCategory.value

        val producer = new KafkaProducer[Object, Object](kafkaParams)

        partition.foreach(record => {
          val message = new ProducerRecord[Object, Object](topic, record.userId, formater.to(record))
          producer.send(message)
          })
        })
      }
    )
  }
}




