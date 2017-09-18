bin/kafka-topics --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --create --topic user_info

bin/kafka-topics --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --create --topic enriched_user

bin/kafka-topics --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --create --topic user_update cleanup.policy=compact