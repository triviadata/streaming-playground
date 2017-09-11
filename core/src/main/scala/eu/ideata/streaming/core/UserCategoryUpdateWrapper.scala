package eu.ideata.streaming.core

import org.apache.avro.generic.GenericData

case class UserCategoryUpdateWrapper(userId: String, category: String, timestamp: Long){
  def asJava: UserCategoryUpdate = new UserCategoryUpdate(userId.toCharArray, category.toCharArray, timestamp)

}

case class UserInfoWrapper(userId: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int){
  def asJava: UserInfo = new UserInfo(userId.toCharArray, timestamp, booleanFlag, subCategory.toCharArray, someValue, intValue)
}

case class UserInfoWithCategoryWrapper(userId: String, category: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int, updated: Long, streamingSource: String) {
  def asJava: UserInfoWithCategory = new UserInfoWithCategory(userId.toCharArray, category.toCharArray, timestamp, booleanFlag, subCategory.toCharArray, someValue, intValue, updated, streamingSource.toCharArray)
}


object UserCategoryUpdateWrapper {
  def fromJava(j: UserCategoryUpdate): UserCategoryUpdateWrapper = {
    UserCategoryUpdateWrapper(j.getUserId.toString, j.getCategory.toString, j.getTimestamp)
  }
}

object UserInfoWrapper {
  def fromJava(j: UserInfo): UserInfoWrapper = {
    UserInfoWrapper(j.getUserId.toString, j.getTimestamp, j.getBooleanFlag, j.getSubCategory.toString, j.getSomeValue, j.getIntValue)
  }
}

object UserInfoWithCategoryWrapper {
  def fromJava(j: UserInfoWithCategory): UserInfoWithCategoryWrapper  =
    UserInfoWithCategoryWrapper(j.getUserId.toString, j.getCategory.toString, j.getTimestamp, j.getBooleanFlag, j.getSubCategory.toString, j.getSomeValue, j.getIntValue, j.getUpdated, j.getStreamingSource.toString)
}