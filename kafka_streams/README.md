

# KSQL Support 

Since the generator is also compatible with newer kafka versions. We can recreate the functionality also using ksql (https://github.com/confluentinc/ksql). The downside of this approach is, that currently ksql does not support querying topics stored as avro. 
For our purposes the generator should be configured to produce json messages. This can be achieved by passing --messageFormat json parameter while running the generator. 

## Requirements
* confluent 3.3.0
* ksql

## Setup
* download confluent 3.3.0
* extract
* run: bin/confluent start
* clone https://github.com/confluentinc/ksql
* cd ksql
* mvn clean compile install -DskipTests
* ./bin/ksql-cli local
* create the following tables

```sql 
CREATE STREAM user_info_original (userId STRING, timestamp BIGINT, booleanFlag BOOLEAN, subCategory STRING, someValue DOUBLE, intValue INTEGER) WITH (kafka_topic='user_info', value_format='JSON');
```

```sql

CREATE TABLE user_category_update_original (userId STRING, category STRING, timestamp BIGINT) WITH (kafka_topic='user_update', value_format='JSON');
```

```sql
CREATE STREAM enriched_user_original WITH (kafka_topic='enriched_user_kafka_stream', value_format='JSON') as SELECT user_info_original.userId, user_category_update_original.category, user_info_original.timestamp, booleanFlag, subCategory, someValue, intValue from user_info_original LEFT JOIN user_category_update_original ON user_info_original.userId = user_category_update_original.userId;
```
