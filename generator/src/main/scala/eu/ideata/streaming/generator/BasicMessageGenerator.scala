package scala.eu.ideata.streaming.generator

import java.time.Instant
import java.util.Properties

import scala.util.Random
import akka.actor.Actor
import akka.event.{Logging, LoggingAdapter}
import eu.ideata.streaming.core.{UserCategoryUpdateWrapper, UserInfoWrapper}
import eu.ideata.streaming.main.{AvroMessage, JsonMessage, MessageFormat}
import eu.ideata.streaming.messages.{GenerateUserCategoryUpdate, GenerateUserInfo, InitialUpdate}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import io.circe.generic.auto._
import io.circe.syntax._

class BasicMessageGenerator(val usersFrom: Int, val usersTo: Int, val categoryModulo: Int, val userInfoTopic: String, userUpateTopic: String, val props: Properties, messageFormat: MessageFormat) extends Actor {

  val log: LoggingAdapter = Logging(context.system, this)

  var total: Long = 0

  val generators = new DataGenerators(usersFrom, usersTo, categoryModulo, log)


  var avroProducer: KafkaProducer[Object, Object] = _
  var jsonProducer: KafkaProducer[String, String] = _


  messageFormat match {
    case AvroMessage => avroProducer = new KafkaProducer[Object, Object](props)
    case JsonMessage => jsonProducer = new KafkaProducer[String, String](props)
  }

  def receive = {
    case GenerateUserInfo => {

      val users = generators
        .generateUserInfo

      val size = {
        messageFormat match {
          case AvroMessage => users
            .map(info => new ProducerRecord[Object, Object](userInfoTopic, info.userId, info.asJava))
            .map(avroProducer.send)

          case JsonMessage => users.map(info =>  new ProducerRecord[String, String](userInfoTopic, info.userId, info.asJson.noSpaces))
            .map(jsonProducer.send)
        }
      }.size


      total += size

      log.info(s"GenerateUserInfo generated: ${size} formated as: ${messageFormat} Total user info sent: ${total}")
    }

    case GenerateUserCategoryUpdate => {

      val users = generators
        .generateUserUpdate

      val size = {
        messageFormat match {
          case AvroMessage =>
            users.map(update => new ProducerRecord[Object, Object](userUpateTopic, update.userId, update.asJava))
            .map(avroProducer.send)

          case JsonMessage =>
            users.map(update => new ProducerRecord[String, String](userUpateTopic, update.userId, update.asJson.noSpaces))
              .map(jsonProducer.send)
        }
      }.size

      log.info(s"GenerateUserCategoryUpdate ${size} messages")

    }
    case InitialUpdate => {
      val users = generators
        .generateInitialUpdate


      val size = {
        messageFormat match {
          case AvroMessage =>
            users.map(update => new ProducerRecord[Object, Object](userUpateTopic, update.userId, update.asJava))
              .map(avroProducer.send)

          case JsonMessage =>
            users.map(update => new ProducerRecord[String, String](userUpateTopic, update.userId, update.asJson.noSpaces))
              .map(jsonProducer.send)
        }
      }.size

      log.info(s"GenerateUserCategoryUpdate Initialization ${size} messages")

    }
  }
}

class DataGenerators(val userIdFrom: Int, val userIdTo: Int, categoryModulo: Int, log: LoggingAdapter){

  lazy val r = new Random(77)

  lazy val users: List[String] = Range(userIdFrom, userIdTo).map(i => sha256Hash(s"user$i")).toList

  log.info(s"User count: ${users.size}")

  def generateUserInfo: List[UserInfoWrapper] = {
    val size = r.nextInt(users.size / 3)
    log.info(s"Random ${size} user info")

    r.shuffle(users).take(size).map(randomUserInfo)
  }

  def generateUserUpdate: List[UserCategoryUpdateWrapper] = r.shuffle(users).take(users.size / 10).map(randomUserCategory)

  def sha256Hash(text: String) : String = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))

  def randomUserInfo(user: String): UserInfoWrapper = {
    val subCategory = r.nextInt(3).toString
    UserInfoWrapper(user, Instant.now().toEpochMilli, r.nextBoolean(), subCategory, r.nextFloat(), r.nextInt(1000))
  }

  def generateInitialUpdate: List[UserCategoryUpdateWrapper] = r.shuffle(users).take((users.size * 0.95).toInt).map(randomUserCategory)

  def randomUserCategory(user: String): UserCategoryUpdateWrapper = {
    val category = r.nextInt(1024).toString
    val timestamp = Instant.now().toEpochMilli
    UserCategoryUpdateWrapper(user, category, timestamp)
  }
}
