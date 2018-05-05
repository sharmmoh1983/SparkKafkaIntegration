import java.util.Properties
import kafka.producer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

/* reads from the kafka topic at every 5 seconds and do the word count
 * reads the hbase table and consolidate the existing word count data and write it back to Hbase
 */
import org.apache.spark.sql.SparkSession
object Spark {
      // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("SparkDemo")
      .master("local[*]")
      .getOrCreate()
      
  
    import spark.implicits._
     val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
}



object SparkKafkaHbase {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    if (args.length < 4) {
      System.err.println("Usage: KafkaVoteCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

   val Array(zkQuorum, group, topics, numThreads) = args
  //  Spark.ssc.checkpoint("checkpoint")
   
    val spark = SparkSession
      .builder()
      .appName("SparkDemo")
      .master("local[*]")
      .getOrCreate()
      
  
    import spark.implicits._
     val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    
    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
      val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)
    val words = lines.flatMap(x => x.split(" "))
    val wordCounts = words.map(x => (x, 1))
      .reduceByKey((acc , value) => (acc + value))
      
  println("Push to hbase....")      
  wordCounts.foreachRDD(rdd => hbasePush(rdd))
  Spark.ssc.start()
  Spark.ssc.awaitTermination()
  }
  
  def hbasePush(rdd: RDD[(String, Int)]) = {
    
   
      val hbaseTableName = "vote_aggregate"
      val hbaseColumnName = "vote_count"
      val hconf = HBaseConfiguration.create()     
      hconf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
      val admin = new HBaseAdmin(hconf)

          if (admin.isTableAvailable(hbaseTableName)) {
            val hbaseRdd= Spark.ssc.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(result => (Bytes.toString(result._2.getRow), 
                     Bytes.toInt((result._2.getValue("CF".getBytes , hbaseColumnName.getBytes)))))
            println("hbaseRdd : ")      
            hbaseRdd.foreach(println)  
            val merged_data =   hbaseRdd.union(rdd).reduceByKey(_ + _ )
            merged_data.foreach(println)                  
            merged_data.foreach( value => {
            val table = new HTable(hconf, hbaseTableName)
            val put = new Put(Bytes.toBytes(value._1.toString()))
            put.addColumn(Bytes.toBytes("CF"), Bytes.toBytes(hbaseColumnName), Bytes.toBytes(value._2.toInt))
            table.put(put)
      })
    }
  }
}