// note:
    // spark-shell --master local[2] , it requires at least two cores
    // we requiree a spark streaming context to use spark streaming

// using Spark Streaming Approach(RDDs)

import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext



object example_stateless extends App{

    val sc = new SparkContext(local[2], "sparkStreaming")


    // creating spark streaming context
    val ssc = new StreamingContext(sc, Seconds(5))


    // this is a dstream
    val lines = ssc.socketTextStream( "localhost", 9998)

    // words is a transformed dstream
    val words = lines.flatMap( x => x.spli(" "))
    val pairs = words.map( x => (x,1))
    val wordcounts = pairs.reduceByKey( (x,y) => (x+y))
    wordcounts.print()

    // starting stream 
    ssc.start()



}

