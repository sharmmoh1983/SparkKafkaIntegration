
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
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
object hbaseConf {
      val hbaseTableName = "device_aggregate"
      val hbaseColumnName = "device_count"
      val hconf = HBaseConfiguration.create()     
      hconf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
     // val admin = new HBaseAdmin(hconf)
}

object SparkConsumer {
  
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
  


  import org.apache.spark.sql.ForeachWriter
  val countWriter = new ForeachWriter[org.apache.spark.sql.Row] {
  override def open(partitionId: Long, version: Long) = true
  override def process(value: org.apache.spark.sql.Row) { 
   // val hConf = new HBaseConfiguration()
 
//hConf.set("hbase.zookeeper.quorum", "localhost:2181")
 //val tableName = "device_aggregate"
 
//val hTable = new HTable(hConf, tableName)
  // val table = new HTable(hConf, tableName)
    //   if(value.get(0) != null ) {
      //    println("In put")
       /*   val hbaseConf = HBaseConfiguration.create()
          hbaseConf.set("hbase.zookeeper.quorum", "localhost")
           hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
           val admin = new HBaseAdmin(hbaseConf)
           println("In put")
 /*  if (admin.tableExists("mytable")){
      println("In put1")
      admin.disableTable("mytable")
      admin.deleteTable("mytable")
      }*/
 println("In put2")
    val tableDesc = new HTableDescriptor(Bytes.toBytes("mytable"))
    val idsColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("ids"))
    tableDesc.addFamily(idsColumnFamilyDesc)
    admin.createTable(tableDesc)

    // let's insert some data in 'mytable' and get the row
 println("In put3")
    val table = new HTable(hbaseConf, "mytable")
 println("In put4")
    val theput= new Put(Bytes.toBytes("rowkey1"))
    println("In put")
    theput.add(Bytes.toBytes("ids"),Bytes.toBytes("id1"),Bytes.toBytes("one"))
     table.put(theput)
     table.close()
      println("After put")*/
          
    println("DeviceType :" + value.get(0))
    println("Count :" +value.get(1))
        //    }
   
  }
  override def close(errorOrNull: Throwable) = {
   // println("If block close")
    
  }
}
  
  val counts = result.
  withWatermark("dateTime", "10 minutes").
  groupBy("Type").
  agg(count("*") as "total")
      


  
  val sq1 = counts.writeStream
  .format("json")
  .foreach(countWriter)
 . outputMode("complete")
  .option("checkpointLocation", "/tmp/checkpoint")
  .start()
  
  

 val query2: StreamingQuery = result.writeStream
   .outputMode("append")
    .format("console")
    .start()
 query2.awaitTermination()


 
  
   }
 }
