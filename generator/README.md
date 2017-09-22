## Kafka Topics

bin/kafka-topics --zookeeper localhost:2181 --replication-factor 3 --partitions 20 --create --topic user_events

bin/kafka-topics --zookeeper localhost:2181 --replication-factor 3 --partitions 20 --create --topic events_enriched

bin/kafka-topics --zookeeper localhost:2181 --replication-factor 3 --partitions 20 --create --topic user_database cleanup.policy=compact

## How to run the generator as fat jar
java -jar generator-assembly-0.1.0-SNAPSHOT.jar --userIdFrom 1 --userIdTo 100000 --userInfoTopic events_persistent --userInfoUpdateTopic users --kafakServerUrl metis-worker2.metis.ideata:9092 --schemaRegistryUrl http://metis-worker1.metis.ideata:8081 --userInfoGenerationIterval 2000 --userUpdateGenerationIterval 3600000 --categoryCount 100 --messageFormat avro
java -jar generator-assembly-0.1.0-SNAPSHOT.jar --userIdFrom 100000 --userIdTo 200000 --userInfoTopic events_persistent --userInfoUpdateTopic users --kafakServerUrl metis-worker2.metis.ideata:9092 --schemaRegistryUrl http://metis-worker1.metis.ideata:8081 --userInfoGenerationIterval 2000 --userUpdateGenerationIterval 3600000 --categoryCount 100 --messageFormat avro
java -jar generator-assembly-0.1.0-SNAPSHOT.jar --userIdFrom 200000 --userIdTo 300000 --userInfoTopic events_persistent --userInfoUpdateTopic users --kafakServerUrl metis-worker2.metis.ideata:9092 --schemaRegistryUrl http://metis-worker1.metis.ideata:8081 --userInfoGenerationIterval 2000 --userUpdateGenerationIterval 3600000 --categoryCount 100 --messageFormat avro
java -jar generator-assembly-0.1.0-SNAPSHOT.jar --userIdFrom 400000 --userIdTo 500000 --userInfoTopic events_persistent --userInfoUpdateTopic users --kafakServerUrl metis-worker2.metis.ideata:9092 --schemaRegistryUrl http://metis-worker1.metis.ideata:8081 --userInfoGenerationIterval 2000 --userUpdateGenerationIterval 3600000 --categoryCount 100 --messageFormat avro

##Topic offloading using kafka-connect
* resources/hdfs-sink.properties