file:///C:/Users/pp255070/OneDrive%20-%20Teradata/Documents/sparklearning/Kafka/ProducerApp/MyProducer.java
### java.util.NoSuchElementException: next on empty iterator

occurred in the presentation compiler.

action parameters:
uri: file:///C:/Users/pp255070/OneDrive%20-%20Teradata/Documents/sparklearning/Kafka/ProducerApp/MyProducer.java
text:
```scala
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

        producer.send(new ProducerRecord<Integer,String>("all_orders", 1, "23:"));

        // step-4 close the producer object
        producer.close();



    }

}
```



#### Error stacktrace:

```
scala.collection.Iterator$$anon$19.next(Iterator.scala:973)
	scala.collection.Iterator$$anon$19.next(Iterator.scala:971)
	scala.collection.mutable.MutationTracker$CheckedIterator.next(MutationTracker.scala:76)
	scala.collection.IterableOps.head(Iterable.scala:222)
	scala.collection.IterableOps.head$(Iterable.scala:222)
	scala.collection.AbstractIterable.head(Iterable.scala:933)
	dotty.tools.dotc.interactive.InteractiveDriver.run(InteractiveDriver.scala:168)
	scala.meta.internal.pc.MetalsDriver.run(MetalsDriver.scala:45)
	scala.meta.internal.pc.PcCollector.<init>(PcCollector.scala:45)
	scala.meta.internal.pc.PcSemanticTokensProvider$Collector$.<init>(PcSemanticTokensProvider.scala:61)
	scala.meta.internal.pc.PcSemanticTokensProvider.Collector$lzyINIT1(PcSemanticTokensProvider.scala:61)
	scala.meta.internal.pc.PcSemanticTokensProvider.Collector(PcSemanticTokensProvider.scala:61)
	scala.meta.internal.pc.PcSemanticTokensProvider.provide(PcSemanticTokensProvider.scala:90)
	scala.meta.internal.pc.ScalaPresentationCompiler.semanticTokens$$anonfun$1(ScalaPresentationCompiler.scala:99)
```
#### Short summary: 

java.util.NoSuchElementException: next on empty iterator