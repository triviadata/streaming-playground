package eu.ideata.streaming.core

case class UserCategoryUpdate(userId: String, category: String, timestamp: Long)

case class UserInfo(userId: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int)

case class UserInfoWithCategory(userId: String, category: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int)