package eu.ideata.streaming.main

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import eu.ideata.streaming.messages.{GenerateUserCategoryUpdate, GenerateUserInfo, InitialUpdate}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.eu.ideata.streaming.generator.BasicMessageGenerator

sealed trait MessageFormat
object AvroMessage extends MessageFormat
object JsonMessage extends MessageFormat

object Main {


  def main(args: Array[String]): Unit = {

    val system = ActorSystem("generate_data")

    implicit val ec = scala.concurrent.ExecutionContext.global

    val appConfig = config(args)
    val kafkaProps = getProps(appConfig, appConfig.messageFormat)

    val actorProps = Props( new BasicMessageGenerator(appConfig.userIdFrom, appConfig.userIdTo, appConfig.categoryCount, appConfig.userInfoTopic, appConfig.userInfoUpdateTopic, kafkaProps, appConfig.messageFormat))

    val actor = system.actorOf(actorProps)

    actor ! InitialUpdate

    system.scheduler.schedule(1000 millis, appConfig.userInfoGenerationIterval millis, actor, GenerateUserInfo)
    system.scheduler.schedule(2000 millis, appConfig.userUpdateGenerationIterval millis, actor, GenerateUserCategoryUpdate)

    Await.result(system.whenTerminated, 10 days)

  }

  case class GeneratorConfig(userIdFrom: Int,
                             userIdTo: Int,
                             categoryCount: Int,
                             userInfoTopic: String,
                             userInfoUpdateTopic: String,
                             kafakServerUrl: String,
                             schemaRegistryUrl: String,
                             userInfoGenerationIterval: Int = 100,
                             userUpdateGenerationIterval: Int = 180000,
                             messageFormat: MessageFormat = JsonMessage
                            )


  def getProps(config: GeneratorConfig, messageFormat: MessageFormat): Properties = {

    val props = new Properties()

    messageFormat match {
      case AvroMessage =>
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])

      case JsonMessage =>
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer])
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer])

    }


    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafakServerUrl)
    props.put("schema.registry.url", config.schemaRegistryUrl)

    props
  }

  def config(xs: Array[String]): GeneratorConfig = {

    lazy val emptyConfig = GeneratorConfig(1, 1000, 100, "user_info", "user_update", "localhost:9092", "http://localhost:8081")

    lazy val parser = new OptionParser[GeneratorConfig]("scopt") {
      head("scopt", "3.x")

      opt[Int]("userIdFrom")
        .optional()
        .action {
          case (s, conf) => conf.copy(userIdFrom = s)
        }

      opt[Int]("userIdTo")
        .optional()
        .action {
          case (s, conf) => conf.copy(userIdTo = s)
        }

      opt[String]("userInfoTopic")
        .optional()
        .action {
          case (s, conf) => conf.copy(userInfoTopic = s)
        }

      opt[String]("userInfoUpdateTopic")
        .optional()
        .action {
          case (s, conf) => conf.copy(userInfoUpdateTopic = s)
        }

      opt[String]("kafakServerUrl")
        .optional()
        .action {
          case (s, conf) => conf.copy(kafakServerUrl = s)
        }

      opt[String]("schemaRegistryUrl")
        .optional()
        .action {
          case (s, conf) => conf.copy(schemaRegistryUrl = s)
        }

      opt[Int]("userInfoGenerationIterval")
        .optional()
        .action {
          case (s, conf) => conf.copy(userInfoGenerationIterval = s)
        }.text("In seconds")

      opt[Int]("userUpdateGenerationIterval")
        .optional()
        .action {
          case (s, conf) => conf.copy(userUpdateGenerationIterval = s)
        }.text("In seconds")

      opt[Int]("categoryCount")
        .optional()
        .action {
          case (s, conf) => conf.copy(categoryCount = s)
        }

      opt[String]("messageFormat")
        .validate{
          case s if(s == "json" | s == "avro") => Right(s)
          case _ => Left("Valid options are json or avro")
        }.action{
          case ("json", conf) => conf.copy(messageFormat = JsonMessage)
          case ("avro", conf) => conf.copy(messageFormat = AvroMessage)

      }
    }
    parser.parse(xs, emptyConfig).getOrElse(emptyConfig)
  }
}
