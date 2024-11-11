import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext



object example_stateful_slidingwindow extends App{

    val sc = new SparkContext(local[2], "sparkStreaming")


    // creating spark streaming context
    val ssc = new StreamingContext(sc, Seconds(2))


    // this is a dstream
    val lines = ssc.socketTextStream( "localhost", 9998)

    // words is a transformed dstream
    val words = lines.flatMap( x => x.split(" "))
    val pairs = words.map( x => (x,1))

    def summaryfunc( x:Int, y:Int)={
        x+y
    }

    def inversefunc( x:Int, y:Int) = {
        x-y
    }
    // for sliding window transformation
    //val wordcounts = pairs.reduceByKeyAndWindow( (x,y) => x+y , (x,y) => x-y, Seconds(10), Seconds(2)).filte( x => (x._2 > 0))
// different way of achieving the above
    val wordcounts = pairs.reduceByKeyAndWindow( summaryfunc(_,_) , inversefunc(_,_), Seconds(10), Seconds(2)).filter( x => (x._2 > 0))
    wordcounts.print()

    // starting stream 
    ssc.start()



}
