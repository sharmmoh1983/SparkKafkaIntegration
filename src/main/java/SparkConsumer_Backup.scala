
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes


import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor }
import scala.reflect.io.Streamable.Bytes

object hbaseConf1 {
      val hbaseTableName = "device_aggregate"
      val hbaseColumnName = "device_count"
      val hconf = HBaseConfiguration.create()     
      hconf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
     // val admin = new HBaseAdmin(hconf)
}

object SparkConsumer1 {
  
   def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder
    .appName("StreamLocallyExample")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val ds1: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "my-connect-test")
    .option("failOnDataLoss", false)
    .load()
    
    //ds1.write.format("text").option("header", "true").save("/user/mohitsharma/file.csv")

    val schema = StructType(Seq(StructField("id", StringType),
      StructField("dateTime", StringType),  StructField("Type", StringType), StructField("Message", StringType)))
   /* val result = ds1.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json($"json", schema).as("data"))
     .select("data.*")*
     * 
     */
     
  //  result.toDF().withColumn("t", current_timestamp()).show

   /* val streamingData = ds1.select(get_json_object(($"value").cast("string"), "$.Type").alias("types"))
                           .count()*/
      
      
      
      val result = ds1.
  select('value cast "string").
  withColumn("tokens", split('value, ",")).
  withColumn("id", ('tokens(0))).
  withColumn("dateTime", 'tokens(1) cast("timestamp")).
  withColumn("Type", 'tokens(2)).
  withColumn("Message", 'tokens(3)).
  select("id","dateTime","Type", "Message")
  
//val words = result.as[String]
// aggregation with watermark
val counts = result.
  withWatermark("dateTime", "10 minutes").
  groupBy("Type").
  agg(count("*") as "total")
      
  counts.printSchema()
  import org.apache.spark.sql.ForeachWriter
val writer = new ForeachWriter[org.apache.spark.sql.Row] {
  override def open(partitionId: Long, version: Long) = true
  override def process(value: org.apache.spark.sql.Row) { 
    val hConf = new HBaseConfiguration()
 
hConf.set("hbase.zookeeper.quorum", "localhost:2181")
 val tableName = "device_aggregate"
 
val hTable = new HTable(hConf, tableName)
    // val table = new HTable(hbaseConf.hconf, hbaseTableName)
          if(value.get(1) != null ) {
            val put = new Put(Bytes.toBytes(value.get(1).toString()))
            put.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("device_aggregate"), Bytes.toBytes(value.get(1).toString()))
            hTable.put(put)
            }
    println("Inloop")
    println(value.get(0))
    println(value.get(1))
  }
  override def close(errorOrNull: Throwable) = {}
}
  
  
 /* val sq1 = counts.writeStream
 .queryName("server-logs processor")
  .foreach(writer)
  . outputMode("update")
  .start*/
 // sq1.awaitTermination()
  
   // val test1 = counts.select("total" ).selectExpr("CAST(total AS STRING)")
    
  //    println (test1)
  
//val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = counts.rdd
     
    // rows.map(x=>{ println(x)});
 // counts.select("total").alias("val").first()
 
 /*   val ints = result
      .withColumn("t", current_timestamp())
    .withWatermark("t", "5 minutes")
      .groupBy(window($"t", "5 minutes") as "window")
      .agg(count("*") as "total")
      
       val ints1 = result
    //  .withColumn("t", current_timestamp())
     // .withWatermark("t", "5 minutes")
      .groupBy(result("Type"))
      .agg(count("*") as "total")*/
      
    val sq = counts.writeStream.
  format("console").
  option("truncate", false)
 //.option("checkpointLocation", "/tmp/checkpoint")
 // trigger(Trigger.ProcessingTime(30.seconds)).
 .outputMode(OutputMode.Update).  // <-- only Update or Complete acceptable because of groupBy aggregation
  start


//counts.toDF().show()


 /*val query: StreamingQuery = ints.writeStream
    .outputMode("append")
    .format("console")
    .start()
  query.awaitTermination()*/

 val query2: StreamingQuery = result.writeStream
   .outputMode("append")
    .format("console")
    .start()
 query2.awaitTermination()

/*val kafkaUserDF: DataFrame = result.select(to_json(struct(result.columns.map(column):_*)).alias("value"))
 var query1: StreamingQuery =   kafkaUserDF.selectExpr("CAST(value AS STRING)").writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "fortune-cookie")
    .option("checkpointLocation", "hdfs://localhost:9000//user/mohitsharma") 
  .start()
  query1.awaitTermination()*/

 /*val query1: StreamingQuery = result.writeStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "localhost:9092") 
  .option("topic", "fortune-cookie") 
  .option("checkpointLocation", "hdfs://localhost:9000//user/mohitsharma") 
  .start()*/

  // query1.awaitTermination()

  
  
  /*result.writeStream 
  .format("parquet") 
  .option("startingOffsets", "earliest") 
  .option("path", "s3://nest-logs") 
  .option("checkpointLocation", "/path/to/HDFS/dir") 
  .start()*/
  
  
/*  result .writeStream 
  .format("kafka") 
  .option("kafka.bootstrap.servers", "localhost:9092") 
  .option("topic", "nest-camera-stats") 
  .option("checkpointLocation", "/path/to/HDFS/dir") 
  .outputMode("complete") 
  .start()*/
  
  //df.write.mode(SaveMode.Overwrite).parquet(hdfs_master + "user/hdfs/wiki/testwiki")
  println("test")
  
 /*  val query1: StreamingQuery =
  result.writeStream 
  .format("csv") 
 // .option("startingOffsets", "earliest") 
  .option("path", "hdfs://localhost:9000//user/mohitsharma/dataframes.csv") 
  .option("checkpointLocation", "hdfs://localhost:9000//user/mohitsharma") 
  .start()
   query1.awaitTermination()*/
 // ds1.write.parquet("hdfs://0.0.0.0:19000/Sales.parquet");
  
  /* val table = new HTable(hbaseConf.hconf, hbaseConf.hbaseTableName)
            val put = new Put(Bytes.toBytes(count._1.toString()))
            put.addColumn(Bytes.toBytes("CF"), Bytes.toBytes(hbaseConf.hbaseColumnName), Bytes.toBytes(value._2.toInt))
table.put(put)*/
  
 /* val hConf = new HBaseConfiguration()
 
hConf.set("hbase.zookeeper.quorum", "localhost:2182")
 
val tableName = "Streaming_wordcount"
 
val hTable = new HTable(hConf, tableName)
 
val tableDescription = new HTableDescriptor(tableName)*/
 
//tableDescription.addFamily(new HColumnDescriptor("Details".getBytes()))
 
//val thePut = new Put(Bytes.toBytes("Word_count"))
 
//thePut.add(Bytes.toBytes("Word_count"), Bytes.toBytes("Occurances"), Bytes.toBytes(row._2.toString))
 
//hTable.put(thePut)
 
  
   }
 }
