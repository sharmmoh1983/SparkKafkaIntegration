import java.util.Properties
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._


import org.apache.kafka.clients.producer._
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.reflect.io.Streamable.Bytes
import org.apache.spark.sql.ForeachWriter


class KafkaSink (topic:String, servers:String) extends ForeachWriter[org.apache.spark.sql.Row]{
  
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", "localhost:9092")
   kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
   kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
   var producer : KafkaProducer[String,String] = _
    
   def open(partitionId: Long, version: Long) : Boolean ={
     // println("In open")
       producer = new KafkaProducer(kafkaProperties)
        true
      
    }
   def process(value: org.apache.spark.sql.Row) : Unit ={ 
   //   println(value.get(0))
   //    println(value.get(1))
     producer.send(new ProducerRecord("fortune-cookie",value.get(0) +":"+ value.get(1)))
       println("In send")
        
  }
   def close(errorOrNull: Throwable) : Unit  = {
 //   println("If block close")
    producer.close()
    
  }
}


object ScalaConsumerProducerKafka {
  
   def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder
    .appName("StreamLocallyExample")
    .config("spark.master", "local")
    .getOrCreate()
    
    println("test")

  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val ds1: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "my-connect-test")
    .option("failOnDataLoss", false)
    .load()
    
     println("test")
   
    val schema = StructType(Seq(StructField("id", StringType),
      StructField("dateTime", StringType),  StructField("Type", StringType), StructField("Message", StringType)))
 
      
  val result = ds1.
  select('value cast "string").
  withColumn("tokens", split('value, ",")).
  withColumn("id", ('tokens(0))).
  withColumn("dateTime", 'tokens(1) cast("timestamp")).
  withColumn("Type", 'tokens(2)).
  withColumn("Message", 'tokens(3)).
  select("id","dateTime","Type", "Message")
  

 println("test")
  
  
  val counts = result.
  withWatermark("dateTime", "10 minutes").
  groupBy("Type").
  agg(count("*") as "total")
      
val counterWriter = new KafkaSink("fortune-cookie","localhost:9092")

   println("test")
  val sq1 = counts.writeStream
  .format("json")
  .foreach(counterWriter)
 . outputMode("complete")
  .option("checkpointLocation", "/tmp/checkpoint")
  .start()
  
   println("test5")

  
  

 val query2: StreamingQuery = result.writeStream
   .outputMode("append")
    .format("console")
    .start()
 query2.awaitTermination()
 
 
  println("test6")

/*  val query1: StreamingQuery =
  result.writeStream 
  .format("csv") 
 // .option("startingOffsets", "earliest") 
  .option("path", "hdfs://localhost:9000//user/mohitsharma/dataframes.csv") 
  .option("checkpointLocation", "hdfs://localhost:9000//user/mohitsharma") 
  .start()
   query1.awaitTermination()*/
 
  
   }
 }
