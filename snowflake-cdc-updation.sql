-- THIS IS A SAMPLE TEMPLATE FOR ONE TABLE. FOLLOW THIS TEMPLATE FOR ALL THE OTHER TABLES TO MERGE THE CHANGE DATA 
-- COMING FROM SONWFLAKE SINK CONNECTOR

-- Based on: 
-- https://docs.snowflake.com/en/user-guide/data-pipelines-examples.html#transforming-loaded-json-data-on-a-schedule
-- https://docs.snowflake.com/en/sql-reference/sql/merge.html

-- Create the replica table, including extra columns to support replica logic and process trazability
create or replace 
    table "ECOMMERCE"."PUBLIC"."REPLICA_MYSQL_PRODUCTS" 
    ( id string PRIMARY KEY comment 'primary key of the source table'
    , sourcedb_binlog_gtid string comment 'database log position, gtid used in HA MySQL (null in other cases), used for ordering events (RECORD_CONTENT:payload.source.gtid)'
    , sourcedb_binlog_file string comment 'database log position, file log name, used for ordering events (RECORD_CONTENT:payload.source.file)'
    , sourcedb_binlog_pos string comment 'database log position, position in log file, used for ordering events (RECORD_CONTENT:payload.source.pos)'
    , payload variant comment 'data after operation (RECORD_CONTENT:payload.after)'
    , cdc_operation char comment 'CDC registered operation in source DB (RECORD_CONTENT:payload.op)'
    , cdc_source_info variant comment 'Debezium source field, for trazability (RECORD_CONTENT:payload.source)'
    , ts_ms_sourcedb number comment 'the timestamp when database register     the event, not available on database snapshot (RECORD_CONTENT:payload.source.ts_ms)'
    , ts_ms_cdc number comment 'the timestamp when the CDC connector capture the event (RECORD_CONTENT:payload.ts_ms)'
    , ts_ms_replica_sf number comment 'the timestamp when snowflake task fills the record')
comment = 'Replica from CDC over MySQL Inventory Users';

--     "freight_value": 25.51,
--     "order_id": "00125cb692d04887809806618a2a145f",
--     "order_item_id": 1,
--     "price": 109.9,
--     "product_id": "1c0c0093a48f13ba70d0c6b0a9157cb7",
--     "seller_id": "41b39e28db005d9731d9d485a83b4c38",
--     "shipping_limit_date": "2022-03-29T13:05:42Z"
  
create or replace view "ECOMMERCE"."PUBLIC"."PRODUCTS"
as select payload:PRODUCT_CATEGORY_NAME PRODUCT_CATEGORY_NAME,
            payload:PRODUCT_CATEGORY_NAME_ENGLISH PRODUCT_CATEGORY_NAME_ENGLISH
    from "ECOMMERCE"."PUBLIC"."REPLICA_MYSQL_PRODUCTS";
-- Create a stream from CDC events table, to process new events into replica table
create or replace 
    stream "ECOMMERCE"."PUBLIC"."PRODUCTS_CDC_STREAM_REPLICATION" 
    on table PRODUCTS_CDC;

-- After create stream (avoid loss events), process all events available in CDC events table
merge into "ECOMMERCE"."PUBLIC"."REPLICA_MYSQL_PRODUCTS" replica_table
    using 
        (with log_data as (select RECORD_METADATA:key as key, RECORD_CONTENT as content from PRODUCTS_CDC),
            prequery as (select key id
                    , COALESCE(content:source.gtid, '') sourcedb_binlog_gtid
                    , COALESCE(content:source.file, '') sourcedb_binlog_file
                    , content:source.pos sourcedb_binlog_pos
                    , content:after payload
                    , content:op cdc_operation
                    , content:source cdc_source_info
                    , content:source.ts_ms ts_ms_sourcedb
                    , content:ts_ms ts_ms_cdc                        
                from log_data),
            rank_query as (select *
                    , ROW_NUMBER() over (PARTITION BY id 
                        order by ts_ms_cdc desc, sourcedb_binlog_file desc, sourcedb_binlog_pos desc) as row_num
                from prequery)
            select * from rank_query where row_num = 1) event_data
        on replica_table.id = event_data.id
    when not matched and event_data.cdc_operation <> 'd' 
        then insert 
                (id, sourcedb_binlog_gtid, sourcedb_binlog_file, sourcedb_binlog_pos, payload
                , cdc_operation, cdc_source_info, ts_ms_sourcedb, ts_ms_cdc, ts_ms_replica_sf)
            values 
                (event_data.id, event_data.sourcedb_binlog_gtid, event_data.sourcedb_binlog_file
                , event_data.sourcedb_binlog_pos, event_data.payload, event_data.cdc_operation
                , event_data.cdc_source_info, event_data.ts_ms_sourcedb, event_data.ts_ms_cdc
                , date_part(epoch_millisecond, CURRENT_TIMESTAMP))
    when matched and event_data.cdc_operation = 'd'
        then delete
    when matched and event_data.cdc_operation <> 'd'
        then update set id=event_data.id
            , sourcedb_binlog_gtid=event_data.sourcedb_binlog_gtid
            , sourcedb_binlog_file=event_data.sourcedb_binlog_file
            , sourcedb_binlog_pos=event_data.sourcedb_binlog_pos
            , payload=event_data.payload
            , cdc_operation=event_data.cdc_operation
            , cdc_source_info=event_data.cdc_source_info
            , ts_ms_sourcedb=event_data.ts_ms_sourcedb
            , ts_ms_cdc=event_data.ts_ms_cdc
            , ts_ms_replica_sf=date_part(epoch_millisecond, CURRENT_TIMESTAMP);


-- Create task with previous tested query, but read data from the created stream (not CDC events table).
create or replace task ECOMMERCE.PUBLIC.PRODUCTS_CDC_TASK_REPLICATION
	warehouse=ECOMM_WH
	schedule='1 minute'
	when system$stream_has_data('ECOMMERCE.PUBLIC.PRODUCTS_CDC_STREAM_REPLICATION')
	as merge into "ECOMMERCE"."PUBLIC"."REPLICA_MYSQL_PRODUCT_CATEGORIES" replica_table
    using 
        (with log_data as (select RECORD_METADATA:key as key, RECORD_CONTENT as content from PRODUCTS_CDC),
            prequery as (select key:PRODUCT_CATEGORY_NAME id
                    , COALESCE(content:source.gtid, '') sourcedb_binlog_gtid
                    , COALESCE(content:source.file, '') sourcedb_binlog_file
                    , content:source.pos sourcedb_binlog_pos
                    , content:after payload
                    , content:op cdc_operation
                    , content:source cdc_source_info
                    , content:source.ts_ms ts_ms_sourcedb
                    , content:ts_ms ts_ms_cdc                        
                from log_data),
            rank_query as (select *
                    , ROW_NUMBER() over (PARTITION BY id 
                        order by ts_ms_cdc desc, sourcedb_binlog_file desc, sourcedb_binlog_pos desc) as row_num
                from prequery)
            select * from rank_query where row_num = 1) event_data
        on replica_table.id = event_data.id
    when not matched and event_data.cdc_operation <> 'd' 
        then insert 
                (id, sourcedb_binlog_gtid, sourcedb_binlog_file, sourcedb_binlog_pos, payload
                , cdc_operation, cdc_source_info, ts_ms_sourcedb, ts_ms_cdc, ts_ms_replica_sf)
            values 
                (event_data.id, event_data.sourcedb_binlog_gtid, event_data.sourcedb_binlog_file
                , event_data.sourcedb_binlog_pos, event_data.payload, event_data.cdc_operation
                , event_data.cdc_source_info, event_data.ts_ms_sourcedb, event_data.ts_ms_cdc
                , date_part(epoch_millisecond, CURRENT_TIMESTAMP))
    when matched and event_data.cdc_operation = 'd'
        then delete
    when matched and event_data.cdc_operation <> 'd'
        then update set id=event_data.id
            , sourcedb_binlog_gtid=event_data.sourcedb_binlog_gtid
            , sourcedb_binlog_file=event_data.sourcedb_binlog_file
            , sourcedb_binlog_pos=event_data.sourcedb_binlog_pos
            , payload=event_data.payload
            , cdc_operation=event_data.cdc_operation
            , cdc_source_info=event_data.cdc_source_info
            , ts_ms_sourcedb=event_data.ts_ms_sourcedb
            , ts_ms_cdc=event_data.ts_ms_cdc
            , ts_ms_replica_sf=date_part(epoch_millisecond, CURRENT_TIMESTAMP);
            
-- Enable task
ALTER TASK "ECOMMERCE"."PUBLIC"."PRODUCTS_CDC_TASK_REPLICATION" RESUME;

-- Check info about the task executions (STATE and NEXT_SCHEDULED_TIME columns)
-- If you see error "Cannot execute task , EXECUTE TASK privilege must be granted to owner role" 
-- review 00-security.sql script
select *
  from table(ECOMMERCE.information_schema.task_history()) where name='PRODUCTS_CDC_TASK_REPLICATION'
  order by scheduled_time desc ;


-- Check counts (you don't see the same results in event table against the replica table)
select to_char(RECORD_CONTENT:payload.op) cdc_operation, count(*), 'PRODUCTS_CDC' table_name 
    from "ECOMMERCE"."PUBLIC"."PRODUCTS_CDC" group by RECORD_CONTENT:payload.op
union all
select cdc_operation, count(*), 'REPLICA_MYSQL_PRODUCTS' table_name
    from "ECOMMERCE"."PUBLIC"."REPLICA_MYSQL_PRODUCTS" group by cdc_operation
order by table_name, cdc_operation;