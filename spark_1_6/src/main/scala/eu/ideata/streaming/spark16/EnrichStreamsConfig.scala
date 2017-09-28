package eu.ideata.streaming.spark16

import scopt.OptionParser

object EnrichStreamsConfig {
  case class StreamingConfig(seconds: Int, kafkaServerUrl: String, schemaRegistryUrl: String, userInfoTopic: String, userCategoryUpdateTopic: String, zookeeperUrl: String, kafkaTargetTopic: String, fromBeginning: Boolean, local: Boolean, statePartitionCount: Int, checkpointDir: String, applicationId: String, messagesPerSecondPerPartition: Int)

  def getConfig(args: Array[String]): StreamingConfig = {

    lazy val emptyConf = StreamingConfig(5, "localhost:9092", "http://localhost:8081", "user_info", "user_update", "localhost:2181", "enriched_user", false, true, 1, "checkpoint/", "kafka-spark_1_6-enrich-streaming", 50000)

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

      opt[String]("userInfoUpdateTopic")
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

      opt[Boolean]("fromBeginning")
        .optional()
        .action{
          case (s,conf) => conf.copy(fromBeginning =  s)
        }

      opt[Boolean]("local")
        .optional()
        .action{
          case (s,conf) => conf.copy(local = s)
        }
      opt[Int]("statePartitionCount")
        .optional()
        .action{
          case (s,conf) => conf.copy(statePartitionCount = s)
        }
      opt[String]("checkpointDir")
        .optional()
        .action {
          case (s, conf) => conf.copy(checkpointDir = s)
        }

      opt[String]("applicationId")
        .optional()
        .action {
          case (s,conf) => conf.copy(applicationId = s)
        }

      opt[Int]("messagesPerSecondPerPartition")
        .optional()
        .action{
          case (s, conf) => conf.copy(messagesPerSecondPerPartition = s)
        }
    }

    parser.parse(args, emptyConf).getOrElse(emptyConf)
  }
}
