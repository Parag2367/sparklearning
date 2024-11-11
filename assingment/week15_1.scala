import org.apache.spark._
import org.apache.spark.streaming.* 
import org.apache.spark.streaming.StreamingContext

object week15_1 extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "SearchDataWithReduceByKeyAndWindow")
    //creating spark Streaming Context
    val ssc = new StreamingContext(sc, Seconds(2))
    //lines is DStream
    val lines = ssc.socketTextStream("localhost", 1724)
    ssc.checkpoint(".")
/**THESE "NAMED FUNCTION" FOR "reduceByKeyAndWindow" */
    def summaryFunct(x: Int, y: Int) = {
    x + y
}
    def inverseFunct(x: Int, y: Int) = {
    x -y
}
//words is a transformed DStream
    val words = lines.flatMap(x => x.split(" ")).map(x => x.toLowerCase())
    val pairs = words.map(x => (x, 1)).filter(a => a._1.startsWith("big"))
/**these "reduceByKeyAndWindow" with Named Function is a STATEFUL TRANSFORMATION & is working on FEW RDD's*/
    val wordCounts = pairs.reduceByKeyAndWindow(summaryFunct(_, _), inverseFunct(_,_), Seconds(10), Seconds(4))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
}

