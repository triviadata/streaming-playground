spark-submit --class eu.ideata.streaming.spark16.EnrichStreams --master yarn --deploy-mode client --driver-memory 5g --driver-cores 2 --executor-memory 10g --executor-cores 4 --num-executors 5 spark16-assembly-0.1.0-SNAPSHOT.jar --seconds 5 --userInfoTopic events_persistent_large --userInfoUpdateTopic users --kafkaServerUrl metis-worker2.metis.ideata:9092 --schemaRegistryUrl http://metis-worker1.metis.ideata:8081 --zookeeperUrl metis-worker1.metis.ideata:2181/kafka --kafkaTargetTopic events_enriched_large --fromBeginning true --statePartitionCount 20 --checkpointDir hdfs://metis-master1.metis.ideata:8020/user/centos/spark_checkpoint/ --applicationId spark_16_large_1 --local false