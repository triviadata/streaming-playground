package eu.ideata.streaming.main

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import eu.ideata.streaming.messages.{GenerateUserCategoryUpdate, GenerateUserInfo}
import org.apache.kafka.clients.producer.ProducerConfig
import scopt.OptionParser

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.eu.ideata.streaming.generator.BasicMessageGenerator

object Main {


  def main(args: Array[String]): Unit = {

    val system = ActorSystem("generate_data")

    implicit val ec = scala.concurrent.ExecutionContext.global

    val appConfig = config(args)
    val kafkaProps = getProps(appConfig)

    val actorProps = Props( new BasicMessageGenerator(appConfig.userIdFrom, appConfig.userIdTo, appConfig.categoryCount, appConfig.userInfoTopic, appConfig.userInfoUpdateTopic, kafkaProps))

    val actor = system.actorOf(actorProps)

    system.scheduler.schedule(0 millis, appConfig.userInfoGenerationIterval millis, actor, GenerateUserInfo)
    system.scheduler.schedule(0 millis, appConfig.userUpdateGenerationIterval millis, actor, GenerateUserCategoryUpdate)

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
                             userUpdateGenerationIterval: Int = 180000
                            )


  def getProps(config: GeneratorConfig): Properties = {

    val props = new Properties()

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafakServerUrl)
    props.put("schema.registry.url", config.schemaRegistryUrl)

    props
  }

  def config(xs: Array[String]): GeneratorConfig = {

    lazy val emptyConfig = GeneratorConfig(1, 10, 2, "user_info", "user_update", "localhost:9092", "http://localhost:8081")

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
    }
    parser.parse(xs, emptyConfig).getOrElse(emptyConfig)
  }
}
