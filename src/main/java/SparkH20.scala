import java.util.Properties
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import ai.h2o.automl.AutoML;
import ai.h2o.automl.AutoMLBuildSpec

import water.Key

import org.apache.kafka.clients.producer._
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.reflect.io.Streamable.Bytes
import org.apache.spark.sql.ForeachWriter

import org.apache.spark.h2o._



object SparkH20 {
  
   def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder
    .appName("StreamLocallyExample")
    .config("spark.master", "local")
    .getOrCreate()
    
    
    val userSchema = new StructType().add("Date", "string").add("TmaxF","string").add("TminF", "string")
                                 .add("TmeanF", "string").add("PrcpIn", "string").add("SnowIn", "string").add("CDD", "string")
     .add("HDD", "string") .add("GDD", "string")
     
    //val file = spark.readStream.schema(userSchema).csv("/Users/mohitsharma/Downloads/weather_ORD.csv") 
   var file_df = spark.
         readStream.
         schema(userSchema).
         format("csv").
         option("header","true").
         load("/Users/mohitsharma/Downloads/h2odata")           
     import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark.sparkContext)
  spark.sparkContext.setLogLevel("ERROR")
  
  var splits = file_df.randomSplit(Array(0.8, 0.2), 1)
  
  val (train,for_predictions) = (splits(0),splits(1))
  
  println(train);
  
// var train = splits[0]
//var for_predictions = splits[1]

 //splits.randomSplit([0.8, 0.2], 1);
   val schema = StructType(Seq(StructField("id", StringType),
      StructField("dateTime", StringType),  StructField("Type", StringType), StructField("Message", StringType)))
 
       val query2: StreamingQuery = file_df.writeStream
   .outputMode("append")
    .format("console")
    .start()
 query2.awaitTermination()
 
  import h2oContext.implicits._
  val h2oFrame = h2oContext.asH2OFrame(train, "my-frame-name")
    
  //h2oFrame.
 
 val autoMLBuildSpec = new AutoMLBuildSpec()
autoMLBuildSpec.input_spec.training_frame = h2oFrame
autoMLBuildSpec.input_spec.response_column = "SnowIn";
autoMLBuildSpec.build_control.loss = "AUTO"
autoMLBuildSpec.build_control.stopping_criteria.set_max_runtime_secs(5)
import java.util.Date;
val aml = AutoML.makeAutoML(Key.make(), new Date(), autoMLBuildSpec)
AutoML.startAutoML(aml)

AutoML.startAutoML(autoMLBuildSpec).get();

    //save the leader model
    var modelSavePath = "data/";
  //  aml.leader().getMojo().writeTo(new FileOutputStream(new File(modelSavePath + "/" + aml.leader()._key + ".zip")));

    //print the leaderboard
    System.out.println(aml.leaderboard());

    //details of the leader model
System.out.println(aml.leader());
  
   }
 }
