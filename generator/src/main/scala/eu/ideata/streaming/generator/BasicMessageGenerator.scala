package scala.eu.ideata.streaming.generator

import java.time.Instant
import java.util.Properties

import scala.util.Random
import akka.actor.Actor
import akka.event.{Logging, LoggingAdapter}
import com.sksamuel.avro4s.RecordFormat
import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo}
import eu.ideata.streaming.messages.{GenerateUserCategoryUpdate, GenerateUserInfo}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class BasicMessageGenerator(val usersFrom: Int, val usersTo: Int, val categoryModulo: Int, val userInfoTopic: String, userUpateTopic: String, val props: Properties) extends Actor {

  val log: LoggingAdapter = Logging(context.system, this)

  val generators = new DataGenerators(usersFrom, usersTo, categoryModulo, log)
  val userInfoFormat = RecordFormat[UserInfo]
  val userUpdateFormat = RecordFormat[UserCategoryUpdate]

  val producer = new KafkaProducer[Object, Object](props)

  def receive = {
    case GenerateUserInfo => {

      val size = generators
        .generateUserInfo
        .map(info => new ProducerRecord[Object, Object](userInfoTopic, info.userId, userInfoFormat.to(info)))
        .map(producer.send)
        .size

      log.info(s"GenerateUserInfo generated: ${size} messages")
    }

    case GenerateUserCategoryUpdate => {

      val size = generators
        .generateUserUpdate
        .map(update => new ProducerRecord[Object, Object](userUpateTopic, update.userId, userUpdateFormat.to(update)))
        .map(producer.send)
        .size

      log.info(s"GenerateUserCategoryUpdate ${size} messages")

    }
  }
}

class DataGenerators(val userIdFrom: Int, val userIdTo: Int, categoryModulo: Int, log: LoggingAdapter){

  lazy val r = new Random(77)

  lazy val users: List[String] = Range(userIdFrom, userIdTo).map(i => sha256Hash(s"user$i")).toList

  log.info(s"User count: ${users.size}")

  def generateUserInfo: List[UserInfo] = {
    val size = r.nextInt(users.size / 3)
    log.info(s"Random ${size} user info")

    r.shuffle(users).take(size).map(randomUserInfo)
  }

  def generateUserUpdate: List[UserCategoryUpdate] = r.shuffle(users).take(10).map(randomUserCategory)

  def sha256Hash(text: String) : String = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))

  def randomUserInfo(user: String): UserInfo = {
    val subCategory = r.nextInt(3).toString
    UserInfo(user, Instant.now().toEpochMilli, r.nextBoolean(), subCategory, r.nextFloat(), r.nextInt(1000))
  }

  def randomUserCategory(user: String): UserCategoryUpdate = {
    val category = r.nextInt(1024).toString
    val timestamp = Instant.now().toEpochMilli
    UserCategoryUpdate(user, category, timestamp)
  }
}
