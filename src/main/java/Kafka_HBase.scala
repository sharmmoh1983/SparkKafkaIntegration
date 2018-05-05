import org.apache.spark.{ SparkConf, SparkContext }
 
import org.apache.spark.streaming.{Seconds, StreamingContext} 
import org.apache.spark.streaming.Seconds
 
import org.apache.spark.streaming.dstream.DStream
 
import org.apache.spark.streaming.{ State, StateSpec }
 
import org.apache.spark.streaming.kafka010.KafkaUtils
 
import org.apache.kafka.common.serialization.StringDeserializer
 
import org.apache.kafka.clients.consumer.ConsumerRecord
 
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
 
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
 
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor }
 
import org.apache.hadoop.hbase.util.Bytes
 
import org.apache.hadoop.hbase.mapreduce.{ TableInputFormat, TableOutputFormat }
 
import org.apache.hadoop.hbase.client.{ HBaseAdmin, Put, HTable }
 
object Kafka_HBase {
 
def main(args: Array[String]) {
 
val conf = new SparkConf().setMaster("local[2]").setAppName("Kafka_Spark_Hbase")
 
val ssc = new StreamingContext(conf, Seconds(10))
 
/*
 
* Defingin the Kafka server parameters
 
*/
 
val kafkaParams = Map[String, Object](
 
"bootstrap.servers" -> "localhost:9092,localhost:9092",
 
"key.deserializer" -> classOf[StringDeserializer],
 
"value.deserializer" -> classOf[StringDeserializer],
 
"group.id" -> "use_a_separate_group_id_for_each_stream",
 
"auto.offset.reset" -> "latest",
 
"enable.auto.commit" -> (false: java.lang.Boolean))
 
val topics = Array("acadgild_topic") //topics list
 
val kafkaStream = KafkaUtils.createDirectStream[String, String](
 
ssc,
 
PreferConsistent,
 
Subscribe[String, String](topics, kafkaParams))
 
val splits = kafkaStream.map(record => (record.key(), record.value.toString)).flatMap(x => x._2.split(" "))
 
val updateFunc = (values: Seq[Int], state: Option[Int]) => {
 
val currentCount = values.foldLeft(0)(_ + _)
 
val previousCount = state.getOrElse(0)
 
val updatedSum = currentCount+previousCount
 
Some(updatedSum)
 
}
 
//Defining a check point directory for performing stateful operations
 
ssc.checkpoint("hdfs://localhost:9000/WordCount_checkpoint")
 
val cnt = splits.map(x => (x, 1)).reduceByKey(_ + _).updateStateByKey(updateFunc)
 
def toHBase(row: (_, _)) {
 
val hConf = new HBaseConfiguration()
 
hConf.set("hbase.zookeeper.quorum", "localhost:2182")
 
val tableName = "Streaming_wordcount"
 
val hTable = new HTable(hConf, tableName)
 
val tableDescription = new HTableDescriptor(tableName)
 
//tableDescription.addFamily(new HColumnDescriptor("Details".getBytes()))
 
val thePut = new Put(Bytes.toBytes(row._1.toString()))
 
thePut.add(Bytes.toBytes("Word_count"), Bytes.toBytes("Occurances"), Bytes.toBytes(row._2.toString))
 
hTable.put(thePut)
 
}
 
val Hbase_inset = cnt.foreachRDD(rdd => if (!rdd.isEmpty()) rdd.foreach(toHBase(_)))
 
ssc.start()
 
ssc.awaitTermination()
 
}
 
}