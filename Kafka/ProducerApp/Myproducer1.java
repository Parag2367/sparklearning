package Kafka.ProducerApp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class Myproducer1 {

    public static void main(String[]args) {

        //Step1 : set the properties
        //client-id, bootstrap-servers,key:value serializer

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, ConstConfig.appId); // refering using a class as they are constant
        props.put(ProducerConfig.BOOTSTRAP_SERVER_CONFIG,ConstConfig.bootStrapServer); // refering using a class as they are constant
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // step-2 Create object of Kafka Producer
        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer,String>(props);

        // Step-3 Calling the send method on producer object

        for (int i = 1 ; i <= 50 ; i++) {

            producer.send(new ProducerRecord<Integer,String>(ConstConfig.topicName, i, "This is my message no "+i));


        }

        
        // step-4 close the producer object
        producer.close();



    }

} {
    
}
