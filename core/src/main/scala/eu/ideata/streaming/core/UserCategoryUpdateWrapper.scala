package eu.ideata.streaming.core

case class UserCategoryUpdateWrapper(userId: String, category: String, timestamp: Long){
  def asJava: UserCategoryUpdate = new UserCategoryUpdate(userId, category, timestamp)
}

case class UserInfoWrapper(userId: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int){
  def asJava: UserInfo = new UserInfo(userId, timestamp, booleanFlag, subCategory, someValue, intValue)
}

case class UserInfoWithCategoryWrapper(userId: String, category: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int, updated: Long, streamingSource: String) {
  def asJava: UserInfoWithCategory = new UserInfoWithCategory(userId, category, timestamp, booleanFlag, subCategory, someValue, intValue, updated, streamingSource)
}


object UserCategoryUpdateWrapper {
  def fromJava(j: UserCategoryUpdate): UserCategoryUpdateWrapper = {
    UserCategoryUpdateWrapper(j.getUserId, j.getCategory, j.getTimestamp)
  }
}

object UserInfoWrapper {
  def fromJava(j: UserInfo): UserInfoWrapper = {
    UserInfoWrapper(j.getUserId, j.getTimestamp, j.getBooleanFlag, j.getSubCategory, j.getSomeValue, j.getIntValue)
  }
}

object UserInfoWithCategoryWrapper {
  def fromJava(j: UserInfoWithCategory): UserInfoWithCategoryWrapper  =
    UserInfoWithCategoryWrapper(j.getUserId, j.getCategory, j.getTimestamp, j.getBooleanFlag, j.getSubCategory, j.getSomeValue, j.getIntValue, j.getUpdated, j.getStreamingSource)
}