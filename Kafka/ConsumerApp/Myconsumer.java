import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;


public class Myconsumer {
    
    public static void main(String[] args) {

        //set the properties

        Properties consumerprops = new Properties();
        //Mandatory
        consumerprops.put(ConsumerConfig.CLIENT_ID_CONFIG,ConstConfig.appId);
        consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVER_CONFIG,ConstConfig.bootStrapServer);
        consumerprops.put(ConsumerConfig.KEY_DESRIALIZER_CLASS_CONFIG,IntegerDeserializer.class.getName());
        consumerprops.put(ConsumerConfig.VALUE_DESRIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        // Optional
        consumerprops.put(ConsumerConfig.GROUP_ID_CONFIG,"CONSUMER_GROUP1");
        consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //  step 2: create object of kafka consumer

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(consumerprops);
        
        // step 3 : subscribe to a topic

        consumer.subscribe(Arrays.asList(ConstConfig.topicName));



        // producer part
        Properties producerprops = new Properties();
        producerprops.put(ProducerConfig.CLIENT_ID_CONFIG,"producer-id1");
        producerprops.put(ProducerConfig.BOOTSTRAP_SERVER_CONFIG,"localhost:9092,localhost:9093");
        producerprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
        producerprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // step-2 Create object of Kafka Producer
        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer,String>(producerprops);




        // consumer records
        while(true){

            ConsumerRecord<Integer, String> records = consumer.poll(100);//every 100 milliseconds

            for (ConsumerRecord<Integer, String> record: records){

                if(record.value().split(",")[3].equals("CLOSED"))// as records are an array so we chose 4th element
                {

                    producer.send(new ProducerRecord<Integer, String>("closed_orders", record.key(), record.value()));

                }
                else
                {
                    producer.send(new ProducerRecord<Integer, String>("completed_orders", record.key(), record.value()));
                }
            }

        }
        // ConsumerRecord<Integer, String> records = consumer.poll(100)


    }

}
