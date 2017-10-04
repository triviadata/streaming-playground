package eu.ideata.streaming.wrapper


import java.time.Instant

import eu.ideata.streaming.core.{UserCategoryUpdate, UserInfo, UserInfoWithCategory}

case class UserCategoryUpdateWrapper(userId: String, category: String, timestamp: Long){
  def asJava: UserCategoryUpdate = new UserCategoryUpdate(userId.toCharArray, category.toCharArray, timestamp)

}

case class UserInfoWrapper(userId: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int, readTimeStamp: Long, intValue2: Int, someValue2: Float, booleanFlag2: Boolean, intValue3: Int, someValue3: Float, booleanFlag3: Boolean, intValue4: Int, someValue4: Float, booleanFlag4: Boolean, subCategory2: String, intValue5: Int, someValue5: Float, booleanFlag5: Boolean, intValue6: Int, someValue6: Float, booleanFlag6: Boolean, intValue7: Int, someValue7: Float, booleanFlag7: Boolean, someValue8: Float, booleanFlag8: Boolean, intValue9: Int, someValue9: Float, intValue10: Int, someValue10: Float){
  def asJava: UserInfo = new UserInfo(userId,timestamp,booleanFlag, subCategory.toCharArray, someValue, intValue, intValue2, someValue2, booleanFlag2, intValue3, someValue3, booleanFlag3, intValue4, someValue4, booleanFlag4, subCategory2.toCharArray, intValue5, someValue5, booleanFlag5, intValue6, someValue6, booleanFlag6, intValue7, someValue7, booleanFlag7, someValue8, booleanFlag8, intValue9, someValue9, intValue10, someValue10)

}

case class UserInfoWithCategoryWrapper(userId: String, category: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int, updated: Long, streamingSource: String, readTimeStamp: Long) {
  def asJava: UserInfoWithCategory = new UserInfoWithCategory(userId.toCharArray, category.toCharArray, timestamp, booleanFlag, subCategory.toCharArray, someValue, intValue, updated, streamingSource.toCharArray, readTimeStamp)
}


object UserCategoryUpdateWrapper {
  def fromJava(j: UserCategoryUpdate): UserCategoryUpdateWrapper = {
    UserCategoryUpdateWrapper(j.getUserId.toString, j.getCategory.toString, j.getTimestamp)
  }
}

object UserInfoWrapper {
  def fromJavaInitializeReadTimestamp(j: UserInfo): UserInfoWrapper = {
    UserInfoWrapper(j.getUserId.toString,j.getTimestamp, j.getBooleanFlag, j.getSubCategory.toString, j.getSomeValue, j.getIntValue, Instant.now().toEpochMilli, j.getIntValue2, j.getSomeValue2, j.getBooleanFlag2, j.getIntValue3, j.getSomeValue3, j.getBooleanFlag3, j.getIntValue4, j.getSomeValue4, j.getBooleanFlag4, j.getSubCategory2.toString, j.getIntValue5, j.getSomeValue5, j.getBooleanFlag5, j.getIntValue6, j.getSomeValue6, j.getBooleanFlag6, j.getIntValue7, j.getSomeValue7, j.getBooleanFlag7, j.getSomeValue8, j.getBooleanFlag8, j.getIntValue9, j.getSomeValue9, j.getIntValue10, j.getSomeValue10)
  }
}

object UserInfoWithCategoryWrapper {
  def fromJava(j: UserInfoWithCategory): UserInfoWithCategoryWrapper  =
    UserInfoWithCategoryWrapper(j.getUserId.toString, j.getCategory.toString, j.getTimestamp, j.getBooleanFlag, j.getSubCategory.toString, j.getSomeValue, j.getIntValue, j.getUpdated, j.getStreamingSource.toString, j.getReadTimeStamp)
}