java -jar kafkaStreams-assembly-0.1.0-SNAPSHOT.jar --kafkaServerUrl metis-worker2.metis.ideata:9092 --schemaRegistryUrl http://metis-worker1.metis.ideata:8081 --zookeeperUrl metis-worker1.metis.ideata:2181/kafka --kafkaTargetTopic events_enriched --userInfoTopic events_persistent --userCategoryUpdateTopic users --fromBeginning true --streamingSource kafka_streams_9 --applicationId kafka_streams_9 --threads 4