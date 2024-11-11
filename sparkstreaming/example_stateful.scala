import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext



object example_stateful extends App{

    val sc = new SparkContext(local[2], "sparkStreaming")

    // creating spark streaming context
    val ssc = new StreamingContext(sc, Seconds(5))

    // this is a dstream
    val lines = ssc.socketTextStream( "localhost", 9998)

    ssc.checkpoint(".") // we did this as in stateful transformation we have to do checkpointing so that it won't be loss "." is present directory
    def updatefunc( newValues :Seq[Int], previousState: Option[Int]): Option[Int] = {

        val newcount = previousState.getorElse(0) + newValues.sum
        Some(newcount)
    }

    // words is a transformed dstream
    val words = lines.flatMap( x => x.spli(" "))
    val pairs = words.map( x => (x,1))

    // changing reduceByKey as it is a stateless function
    //val wordcounts = pairs.reduceByKey( (x,y) => (x+y))
    //
    //
    // internally it will be like this
    //(big,{1,1})
    //(data,{1,1})
    val wordcounts = pairs.updateStateByKey( updatefunc )   // updateStateByKey takes 2 parameters
    wordcounts.print()

    // starting stream 
    ssc.start()

}
