CREATE TEMPORARY SYSTEM FUNCTION BIN_TO_UUID AS 'cdcjob.function.BinToUuid';

CREATE TEMPORARY TABLE vitess_source_base WITH (
    -- vitess-cdc plus the op_ts(binlog event time) metadata column, see cdcjob.source
    'connector' = 'vitess-cdc-op-ts',
    'hostname' = '${VITESS_VTGATE_HOST}',
    'port' = '${VITESS_VTGATE_GRPC_PORT}',
    'keyspace' = 'conversation',
    -- tablet pool has a single tablet which is the primary
    'tablet-type' = 'MASTER'
);

CREATE TEMPORARY TABLE kafka_sink_base WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'conversation-cdc',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_SERVERS}',
    'properties.security.protocol' = 'SSL',
    'properties.ssl.truststore.type' = 'PEM',
    'properties.ssl.truststore.certificates' = '${KAFKA_CA_CERT}',
    'properties.ssl.keystore.type' = 'PEM',
    'properties.ssl.keystore.certificate.chain' = '${KAFKA_USER_CERT}',
    'properties.ssl.keystore.key' = '${KAFKA_USER_KEY}',
    'value.format' = 'json',
    'sink.delivery-guarantee' = 'at-least-once'
);

-- ------------------------------------------------------------
-- sources
-- ------------------------------------------------------------

CREATE TEMPORARY TABLE offline_conversation_source (
    id             BYTES,
    novel          STRING,
    poem           STRING,
    short_story    STRING,
    play           STRING,
    film           STRING,
    written_by     STRING,
    description    STRING,
    `time`         STRING,
    length_minutes INT,
    maps_link      STRING,
    location       STRING,
    latitude       DOUBLE,
    longitude      DOUBLE,
    city           STRING,
    h3_res5        STRING,
    h3_res7        STRING,
    updated_at     STRING,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.offline_conversation',
    'name' = 'offline_conversation'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE offline_conversation_moderator_source (
    conversation_id BYTES,
    member_id       BYTES,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.offline_conversation_moderator',
    'name' = 'offline_conversation_moderator'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE offline_conversation_participant_source (
    conversation_id BYTES,
    member_id       BYTES,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.offline_conversation_participant',
    'name' = 'offline_conversation_participant'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE online_conversation_source (
    id                  BYTES,
    novel               STRING,
    short_story         STRING,
    poem                STRING,
    play                STRING,
    film                STRING,
    written_by          STRING,
    description         STRING,
    capacity            INT,
    `time`              STRING,
    length_minutes      INT,
    current_registrants INT,
    updated_at          STRING,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.online_conversation',
    'name' = 'online_conversation'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE online_conversation_moderator_source (
    conversation_id BYTES,
    member_id       BYTES,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.online_conversation_moderator',
    'name' = 'online_conversation_moderator'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE online_conversation_registrant_source (
    conversation_id BYTES,
    member_id       BYTES,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.online_conversation_registrant',
    'name' = 'online_conversation_registrant'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE online_conversation_ban_source (
    conversation_id BYTES,
    member_id       BYTES,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.online_conversation_ban',
    'name' = 'online_conversation_ban'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE online_conversation_notification_source (
    conversation_id BYTES,
    member_id       BYTES,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.online_conversation_notification',
    'name' = 'online_conversation_notification'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE outbox_source (
    conversation_id BYTES,
    topic           STRING,
    task_type       STRING,
    payload         STRING,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'table-name' = 'conversation.outbox',
    'name' = 'outbox',
    'insert-only' = 'true'
) LIKE vitess_source_base (EXCLUDING ALL INCLUDING OPTIONS);

-- the outbox of holidayfriend, in its own keyspace: a row is written in the transaction of what it announces, like
-- the ChatMessage of an accepted invite, so the message goes out exactly when the invite is accepted.
-- id is the id of the message, the kafka key. OVERWRITING, as the keyspace of the base is replaced
CREATE TEMPORARY TABLE holidayfriend_outbox_source (
    id        BYTES,
    topic     STRING,
    task_type STRING,
    payload   STRING,
    op_ts TIMESTAMP_LTZ(3) METADATA FROM 'op_ts' VIRTUAL
) WITH (
    'keyspace' = 'holidayfriend',
    'table-name' = 'holidayfriend.outbox',
    'name' = 'holidayfriend_outbox',
    'insert-only' = 'true'
) LIKE vitess_source_base (EXCLUDING ALL OVERWRITING OPTIONS);

-- ------------------------------------------------------------
-- sinks
-- ------------------------------------------------------------

CREATE TEMPORARY TABLE offline_conversation_sink (
    id             STRING,
    novel          STRING,
    poem           STRING,
    short_story    STRING,
    play           STRING,
    film           STRING,
    written_by     STRING,
    description    STRING,
    `time`         STRING,
    length_minutes INT,
    maps_link      STRING,
    location       STRING,
    latitude       DOUBLE,
    longitude      DOUBLE,
    city           STRING,
    h3_res5        STRING,
    h3_res7        STRING,
    updated_at     STRING,
    headers        MAP<STRING, BYTES> METADATA,
    ts             TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'key.format' = 'raw'
) LIKE kafka_sink_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE online_conversation_sink (
    id                  STRING,
    novel               STRING,
    short_story         STRING,
    poem                STRING,
    play                STRING,
    film                STRING,
    written_by          STRING,
    description         STRING,
    capacity            INT,
    `time`              STRING,
    length_minutes      INT,
    current_registrants INT,
    updated_at          STRING,
    headers             MAP<STRING, BYTES> METADATA,
    ts                  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'key.format' = 'raw'
) LIKE kafka_sink_base (EXCLUDING ALL INCLUDING OPTIONS);

-- every member relation table (moderator, participant, registrant, ban, notification) has the same shape
CREATE TEMPORARY TABLE conversation_member_sink (
    conversation_id STRING,
    member_id       STRING,
    headers         MAP<STRING, BYTES> METADATA,
    ts              TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
    PRIMARY KEY (conversation_id, member_id) NOT ENFORCED
) WITH (
    'key.format' = 'json'
) LIKE kafka_sink_base (EXCLUDING ALL INCLUDING OPTIONS);

CREATE TEMPORARY TABLE chat_message_sink (
    conversation_id STRING,
    payload         STRING,
    headers         MAP<STRING, BYTES> METADATA,
    ts              TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'chat-message',
    'key.format' = 'raw',
    'key.fields' = 'conversation_id',
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY'
) LIKE kafka_sink_base (EXCLUDING ALL OVERWRITING OPTIONS);

-- same shape, schedule and cancel events of a conversation stay in order on one partition
CREATE TEMPORARY TABLE scheduled_notification_sink (
    conversation_id STRING,
    payload         STRING,
    headers         MAP<STRING, BYTES> METADATA,
    ts              TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'scheduled-notification',
    'key.format' = 'raw',
    'key.fields' = 'conversation_id',
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY'
) LIKE kafka_sink_base (EXCLUDING ALL OVERWRITING OPTIONS);

-- the chat-message of holidayfriend's own chat services (common.AppScoped in my-server)
CREATE TEMPORARY TABLE holidayfriend_chat_message_sink (
    id      STRING,
    payload STRING,
    headers MAP<STRING, BYTES> METADATA,
    ts      TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'holidayfriend.chat-message',
    'key.format' = 'raw',
    'key.fields' = 'id',
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY'
) LIKE kafka_sink_base (EXCLUDING ALL OVERWRITING OPTIONS);

-- ------------------------------------------------------------
-- pipelines, submitted together as one job
-- ------------------------------------------------------------

INSERT INTO offline_conversation_sink
-- DATETIME is stored in UTC, 'yyyy-MM-dd HH:mm:ss' -> 'yyyy-MM-ddTHH:mm:ssZ'
SELECT BIN_TO_UUID(id), novel, poem, short_story, play, film, written_by, description,
       CONCAT(REPLACE(`time`, ' ', 'T'), 'Z'), length_minutes, maps_link, location, latitude, longitude,
       city, h3_res5, h3_res7, CONCAT(REPLACE(updated_at, ' ', 'T'), 'Z'),
       MAP['taskType', ENCODE('offline_conversation', 'UTF-8')], op_ts
FROM offline_conversation_source;

INSERT INTO online_conversation_sink
SELECT BIN_TO_UUID(id), novel, short_story, poem, play, film, written_by, description, capacity,
       CONCAT(REPLACE(`time`, ' ', 'T'), 'Z'), length_minutes, current_registrants,
       CONCAT(REPLACE(updated_at, ' ', 'T'), 'Z'),
       MAP['taskType', ENCODE('online_conversation', 'UTF-8')], op_ts
FROM online_conversation_source;

INSERT INTO conversation_member_sink
SELECT BIN_TO_UUID(conversation_id), BIN_TO_UUID(member_id),
       MAP['taskType', ENCODE('offline_conversation_moderator', 'UTF-8')], op_ts
FROM offline_conversation_moderator_source;

INSERT INTO conversation_member_sink
SELECT BIN_TO_UUID(conversation_id), BIN_TO_UUID(member_id),
       MAP['taskType', ENCODE('offline_conversation_participant', 'UTF-8')], op_ts
FROM offline_conversation_participant_source;

INSERT INTO conversation_member_sink
SELECT BIN_TO_UUID(conversation_id), BIN_TO_UUID(member_id),
       MAP['taskType', ENCODE('online_conversation_moderator', 'UTF-8')], op_ts
FROM online_conversation_moderator_source;

INSERT INTO conversation_member_sink
SELECT BIN_TO_UUID(conversation_id), BIN_TO_UUID(member_id),
       MAP['taskType', ENCODE('online_conversation_registrant', 'UTF-8')], op_ts
FROM online_conversation_registrant_source;

INSERT INTO conversation_member_sink
SELECT BIN_TO_UUID(conversation_id), BIN_TO_UUID(member_id),
       MAP['taskType', ENCODE('online_conversation_ban', 'UTF-8')], op_ts
FROM online_conversation_ban_source;

INSERT INTO conversation_member_sink
SELECT BIN_TO_UUID(conversation_id), BIN_TO_UUID(member_id),
       MAP['taskType', ENCODE('online_conversation_notification', 'UTF-8')], op_ts
FROM online_conversation_notification_source;

-- outbox rows with a task_type carry it as the "taskType" header, rows without one get no headers
INSERT INTO chat_message_sink
SELECT BIN_TO_UUID(conversation_id), payload,
       CASE WHEN task_type IS NULL THEN CAST(NULL AS MAP<STRING, BYTES>)
            ELSE MAP['taskType', ENCODE(task_type, 'UTF-8')] END,
       op_ts
FROM outbox_source
WHERE topic = 'chat-message';

INSERT INTO scheduled_notification_sink
SELECT BIN_TO_UUID(conversation_id), payload,
       CASE WHEN task_type IS NULL THEN CAST(NULL AS MAP<STRING, BYTES>)
            ELSE MAP['taskType', ENCODE(task_type, 'UTF-8')] END,
       op_ts
FROM outbox_source
WHERE topic = 'scheduled-notification';

-- last, so the operators above keep their place in the job graph and their state restores
INSERT INTO holidayfriend_chat_message_sink
SELECT BIN_TO_UUID(id), payload,
       CASE WHEN task_type IS NULL THEN CAST(NULL AS MAP<STRING, BYTES>)
            ELSE MAP['taskType', ENCODE(task_type, 'UTF-8')] END,
       op_ts
FROM holidayfriend_outbox_source
WHERE topic = 'chat-message';
