# Real-time, high throughput streaming ELT data pipeline for Ecommerce

This project explores and implements an ecommerce use case where there are hundreds of varied data inputs like customer/seller/product creations, orders, reviews, payments etc. At such a high velocity, the data maybe treated as streaming data and hence, a solution for data ingestion, warehousing and analytics must be designed accordingly. Further, to add a real life element, I decided I'll implement _reward points_ for all customers. 

Since the beginning, I was sure I'll use Kinesis for stream ingestion but, using separate streams for each table would become a little too expensive. So what I instead did is put all the data into the same data stream but related data with the same partition key. For instance, partitionkey of "orders" for all orders. While processing the microbatches in Lambda, I'd group the records by their partition keys and form dynamic queries for the relevant table with no mention of any table/partition key in the code. This gave me two huge advantages: 

**-> 7x cost saving on Kinesis by using single data stream instead of 7**

**-> 500x less database connections by writing in microbatches of 500 records**

My plan was to use MySQL on RDS for OLTP and S3 as a stage for Snowflake to pick the data from. But while implementing, I realized that the data is highly dynamic in nature and updates and deletes are going to be just as frequent as inserts. For instance, order cancellations, reward point updates etc. Hence, loading inserts passively from S3 wasn't an option as it'd result in **stale analytics**. 

This is when I decided to keep the data warehouse in sync with the OLTP database using a CDC (change data capure) pipeline using Debezium, Confluent Kafka and Snowflake streams. This is the phase where I learnt the most in this entire project. Namely, I learnt Debezium, CDC, Confluent Kafka, Streams, Merges and Tasks in Snowflake. 

To implement reward points, I'd add points once a delivered order was 11 days old i.e. it was over its return period. A Lambda function scheduled using an Event Bridge cron rule proved to be an ideal fit for the job.

Now that my data extraction and loading were sorted, for analytics I used DBT as it really simplifies the code maintainence, by making it more modular, easy to analyze and portable. The two metrics I implemented here were: **previous month unit sales by category** and **previous month city revenue**

Watching the data flow from source to analytics smoothly within a couple of minutes was for sure worth all the effort.

![alt text](https://github.com/codingvarun/ecommerce-elt-pipeline/blob/main/ecommerce-streaming-elt.jpeg?raw=true)
