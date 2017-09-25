# Overview


Test stateful streaming, and joining streams in various technologies.

## Use case

We have 2 streams:

* **user_info** is a stream with relatively high velocity, contains generated events for users (UserInfo(userId: String, timestamp: Long, booleanFlag: Boolean, subCategory: String, someValue: Float, intValue: Int))


* **user_update** is a stream with relatively low velocity, and it contains some additional information about users (UserCategoryUpdate(userId: String, category: String, timestamp: Long))

Our goal is to join those 2 streams, and create an shared state where we will store the current category for each users, and produce a new stream with the enriched user profile.


### Currently supporting
* spark 1.6.2
* flink 1.3.1
* kafka-streams 3.1.2
* ksql 3.3.0

### Todo:
* spark 2.x
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
        
    * for flink
        * download flink 1.3.1
        * run ${flink_directory}/bin/start-local.sh
        * go to project root directory
        * build fat jar using sbt assembly
        * run ${flink_directory}/bin/flink run flink/target/scala-2.11/flink-assembly-0.1.0-SNAPSHOT.jar
        
    * for kafka-streams
        * sbt kafkaStreams/run
        
    * for ksql
        * check out the guide at kafka_streams/README.md 
        

### Testcase 1: Joining Streams

|                        | Smoke test<sup>1)</sup> (seconds) | Pregenerated Data<sup>2)</sup> (seconds) | Using Data Generator<sup>3)</sup> (seconds) | 
| ---------------------- | --------------------------------: | ----------------------------------------:| ------------------------------------------: |
| Spark Streaming (1.6)  | 10.759                            |                                          |                                             |
| Flink (1.3)            |                                   |                                          |                                             |
| Kafka Streams (3.3.0)  |                                   |                                          |                                             |


<sup>1)</sup> Processing 11.6 mil of pregenerated messages from Kafka topic
<sup>2)</sup> Processing XY mil of pregenerated messages from Kafka topic - same as smoke test but
<sup>3)</sup> Processing XY mil of generated messages. Messages are generated on the fly.