package Kafka.ProducerApp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class MyProducer {

    public static void main(String[]args) {

        //Step1 : set the properties
        //client-id, bootstrap-servers,key:value serializer

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"producer-id1");
        props.put(ProducerConfig.BOOTSTRAP_SERVER_CONFIG,"localhost:9092,localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // step-2 Create object of Kafka Producer
        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer,String>(props);

        // Step-3 Calling the send method on producer object(subscribe to topic)

        producer.send(new ProducerRecord<Integer,String>("all_orders", 1, "23:00:89:24,1211,COMPLETE"));

        // step-4 close the producer object
        producer.close();



    }

}