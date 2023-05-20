# Real-time, high throughput streaming ELT data pipeline for Ecommerce

This project explores and implements an ecommerce use case with numerous data inputs such as customer/seller/product creations, orders, reviews, and payments. Due to the high velocity of data, it is treated as streaming data, requiring a solution for data ingestion, warehousing, and analytics. Additionally, I decided to implement reward points for customers.

To optimize costs, I used a single data stream in Kinesis instead of separate streams for each table. Data with the same partition key was grouped together, such as "orders" for all orders. This approach provided two significant advantages: **a 7x cost saving on Kinesis** and **100-500x fewer database connections by processing microbatches in Lambda.**

Originally, I planned to use MySQL on RDS for OLTP and S3 as a staging area for Snowflake. However, due to the dynamic nature of the data and frequent updates and deletes, passive loading from S3 would result in stale analytics. To address this, I decided to keep the data warehouse in sync with the OLTP database using a CDC pipeline involving Debezium, Confluent Kafka, and Snowflake streams. This phase provided valuable learning opportunities in Debezium, CDC, Confluent Kafka, Streams, Merges, and Tasks in Snowflake.

For implementing reward points, I scheduled a Lambda function using an Event Bridge cron rule to add points once a delivered order was 11 days old (after its return period).

To simplify code maintenance, improve modularity, and enhance portability, I used DBT for analytics. The implemented metrics were previous month unit sales by category and previous month city revenue.

Witnessing the smooth flow of data from source to analytics within minutes made all the effort worthwhile.
Here is the [blog](https://dzone.com/articles/howto_building-an-enterprise-cdc-solution) by [Dario Cazas Pernas](https://github.com/dariocazas) that help me get started with CDC.

![alt text](https://github.com/codingvarun/ecommerce-elt-pipeline/blob/main/ecommerce-streaming-elt.jpeg?raw=true)
