package eu.ideata.streaming.spark16

import scopt.OptionParser

object Config {
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
