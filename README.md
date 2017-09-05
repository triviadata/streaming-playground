#Overview

Test stateful streaming, and joining streams in various technologies.

## Use case

We have 2 streams:

* **user_info** is a stream with relatively high velocity, contains generated events for users (UserInfo(userId: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int))


* **user_update** is a stream with relatively low velocity, and it contains some additional information about users (UserCategoryUpdate(userId: String, category: String, timestamp: Long))

Our goal is to join those 2 streams, and create an shared state where we will store, the current category for each users. And produce a new stream with the enriched user profile.


### Currently supporting
* spark_16

### Todo:
* flink
* spark 2.x
* kafka-streams
* storm
* heron
* apex
* samza


## Requirements

* Confluent platform 3.1.2 (It should work with newer versions, but it is not tested)

## How to run

* follow http://docs.confluent.io/3.1.2/quickstart.html and start
    * zookeeper
    * kafka 
    * schema-registry
    
* create topics
    * user_info
    * user_update
    * enriched_user

* run the generator
    * sbt generator/run
    
* run one of the streaming platforms
    * for spark_1_6
        * sbt spark16/run

