package eu.ideata.streaming.kafkaStreams.utils

import scopt.OptionParser

object Config {
  case class StreamingConfig(kafkaServerUrl: String, schemaRegistryUrl: String, userInfoTopic: String, userCategoryUpdateTopic: String, zookeeperUrl: String, kafkaTargetTopic: String, fromBeginning: Boolean)

  def getConfig(args: Array[String]): StreamingConfig = {

    lazy val emptyConf = StreamingConfig("http://localhost:9092", "http://localhost:8081", "user_info", "user_update", "localhost:2181", "enriched_user", false)

    lazy val parser = new OptionParser[StreamingConfig]("scopt") {

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
        }

      opt[Boolean]("fromBeginning")
        .optional()
        .action{
          case (s, conf) => conf.copy(fromBeginning = s)
        }
    }

    parser.parse(args, emptyConf).getOrElse(emptyConf)
  }
}