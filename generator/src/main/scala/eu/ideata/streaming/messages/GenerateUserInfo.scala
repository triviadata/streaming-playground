package eu.ideata.streaming.messages

sealed trait Messages

object GenerateUserCategoryUpdate extends Messages
object GenerateUserInfo extends Messages
object InitialUpdate extends Messages
