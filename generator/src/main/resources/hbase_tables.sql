CREATE TABLE IF NOT EXISTS user_info(
userId VARCHAR not null,
timestamp bigint not null,
booleanFlag BOOLEAN,
subCategory varchar,
someValue FLOAT,
intValue INTEGER,
CONSTRAINT pk PRIMARY KEY(userId,timestamp)
)


create table user_update(
userId VARCHAR not null,
timestamp timestamp not null,
category VARCHAR,
CONSTRAINT pk PRIMARY KEY(userId,timestamp)
)