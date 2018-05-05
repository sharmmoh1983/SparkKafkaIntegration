

import java.util.{Properties}

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}


object MyProducer {
  
   def main(args: Array[String]): Unit = {
  
  val topic = "my-connect-test"
   val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092");
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
println("Sent: $i")
  val producer = new KafkaProducer[String,String](props);



 try {
   for(i <- 0 to 10) {
     println("Sent: $i")
     producer.send(new ProducerRecord[String, String](topic, "title "+i.toString,"data from topic"))
     println(s"Sent: $i")
   }
   println("Message sent successfully");
       producer.close()
 }
 catch {
   case ex: Exception =>
     ex.printStackTrace()
 }
 
   }
}
