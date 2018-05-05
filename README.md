Starting zookeeper and kafka broker

>bin/zookeeper-server-start.sh config/zookeeper.properties

>bin/kafka-server-start.sh config/server.properties 

Now execute consumerSpark file then run producer

# SparkKafkaIntegration



Install kafka 
cd /Users/mohitsharma/Downloads/kafka_2.11-1.1.0

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic fortune-cookie

bin/zookeeper-server-start.sh config/zookeeper.properties 

bin/kafka-server-start.sh config/server.properties 

nano /tmp/my-test.txt

Copy paste :

1,2016-02-07 10:50:02,Smoke,Detected\n
1,2016-02-07 10:51:02,Smoke,Detected\n
1,2016-01-07 00:01:02,Smoke,Detected\n
1,2016-01-07 00:01:02,Smoke,Detected\n
1,2018-05-02 11:20:02,Temperature,Detected

sh bin/connect-standalone.sh config/my-standalone.properties config/my-file-source.properties 


Install hadoop

Windows: https://www.youtube.com/watch?v=VhxWig96dME

Mac : https://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/


	Hadoop Url:http://localhost:9870

	sudo nano  ~/.profile

	hadoop fs -mkdir

	hstart
	hstop


	Links I followed :
  
  https://chimpler.wordpress.com/2014/07/01/implementing-a-real-time-data-pipeline-with-spark-streaming/


http://amithora.com/spark-update-by-key-explained/


https://acadgild.com/blog/spark-streaming-tcp-socket/


http://blog.antlypls.com/blog/2017/10/15/using-spark-sql-and-spark-streaming-together/

https://github.com/mongodb/mongo-spark/blob/master/doc/6-FAQ.md


https://eradiating.wordpress.com/2016/02/27/aggregating-time-series-with-spark-dataframe/


https://stackoverflow.com/questions/37632238/how-to-group-by-time-interval-in-spark-sql


http://asyncified.io/2017/07/30/exploring-stateful-streaming-with-spark-structured-streaming/​



https://github.com/hollie/misterhouse/wiki/Integration-With-The-Actions-on-Google-Smart-Home-Provider

https://acadgild.com/blog/spark-streaming-tcp-socket/


http://blog.antlypls.com/blog/2017/10/15/using-spark-sql-and-spark-streaming-together/

https://github.com/mongodb/mongo-spark/blob/master/doc/6-FAQ.md


https://eradiating.wordpress.com/2016/02/27/aggregating-time-series-with-spark-dataframe/


https://stackoverflow.com/questions/37632238/how-to-group-by-time-interval-in-spark-sql


http://asyncified.io/2017/07/30/exploring-stateful-streaming-with-spark-structured-streaming/​



https://github.com/hollie/misterhouse/wiki/Integration-With-The-Actions-on-Google-Smart-Home-Provider


http://nilhcem.com/android-things/google-assistant-smart-home


https://github.com/YuvalItzchakov/spark-stateful-example/blob/master/src/main/scala/com/github/yuvalitzchakov/structuredstateful/StatefulStructuredSessionization.scala


https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html


https://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/


http://bigdatums.net/2017/06/20/writing-file-content-to-kafka-topic/



https://cloudxlab.com/blog/real-time-analytics-dashboard-with-apache-spark-kafka/


https://community.mapr.com/community/exchange/blog/2017/04/26/real-time-streaming-data-pipelines-with-apache-apis-kafka-spark-streaming-and-hbase



https://github.com/duyetdev/realtime-dashboard


https://www.youtube.com/watch?v=3hVMXhqYhXc


State :

https://blog.cloudera.com/blog/2017/06/offset-management-for-apache-kafka-with-apache-spark-streaming/


https://github.com/charlsjoseph/VoteCount-Aggregator-using-spark-streaming


Apache HBase is an open source NoSQL distributed database that runs on top of the Hadoop Distributed File System (HDFS). It is well-suited for faster read/write operations on large datasets with high throughput and low input/output latency. But, unlike relational and traditional databases, HBase lacks support for SQL scripting, data types, etc., and requires the Java™ API to achieve the equivalent functionality.

This developer pattern is intended to provide application developers familiar with SQL the ability to access HBase data tables using the same SQL commands. You will quickly learn how to create and query the data tables by using Apache Spark SQL and the HSpark connector package. This allows you to take advantage of the significant performance gains from using HBase without having to learn the Java APIs required to traditionally access the HBase data tables.


https://github.com/IBM/sparksql-for-hbase


https://github.com/hortonworks-spark/shc/issues/205


https://stackoverflow.com/questions/44187297/spark-structured-streaming-processing-each-row


http://kamanja.org/forums/topic/execute-hbase-without-the-in-built-zookeeper-cluster/

​(without zookeeper)

http://hanishblogger.blogspot.com.au/2013/01/configuring-and-running-hbase-in-pseudo.html


http://www.eaiesb.com/blogs/?p=513​


https://stackoverflow.com/questions/35675130/spark-streaming-not-remembering-previous-state?rq=1​



val sparkSess = SparkSession.builder().appName("My App").getOrCreate()
 val sc = sparkSess.sparkContext
 val ssc = new StreamingContext(sc, Seconds(time))​





http://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-7/​


maintain state


https://github.com/knockdata/spark-highcharts/issues/24



https://github.com/mongodb/mongo-spark/blob/master/examples/src/test/scala/tour/SparkStructuredStreams.scala


http://vishnuviswanath.com/spark_structured_streaming.html​

(Great article must read)


https://devcenter.heroku.com/articles/realtime-polyglot-app-node-ruby-mongodb-socketio​



https://databricks.com/blog/2017/04/04/real-time-end-to-end-integration-with-apache-kafka-in-apache-sparks-structured-streaming.html

(write to kafka)


https://stackoverflow.com/questions/47457755/spark-streaming-read-json-from-kafka-and-write-json-to-other-kafka-topic


write to kafka

