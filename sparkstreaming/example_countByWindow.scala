import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext



object example_countByWindow extends App{

    val sc = new SparkContext(local[2], "sparkStreaming")


    // creating spark streaming context
    val ssc = new StreamingContext(sc, Seconds(2))


    // this is a dstream
    val lines = ssc.socketTextStream( "localhost", 9998)
       def summaryfunc( x:String, y:String)={
        (x.toInt + y.toInt).toString
    }

    def inversefunc( x:Int, y:Int) = {
        (x.toInt - y.toInt).toString
    }
    // words is a transformed dstream
    // countByWindow does not need pair rdds that is why no map, flatmap
    val words = lines.countByWindow(Seconds(10), Seconds(2))
    // it will count all the lines for a specified window size
 
    wordcounts.print()

    // starting stream 
    ssc.start()



}
