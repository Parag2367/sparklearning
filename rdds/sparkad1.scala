import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Loglevel extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]" , "loglevel")

    val mylist = List("WARN: this is my",
                    "ERROR: my gpaytk",
                    "ERROR: 28 September",
                    "WARN: this 54 times")

    val rdd1 = sc.parallelize(mylist)

    val rdd2 = rdd1.map(x => {
        val columns = x.split(":")
        val loglevel = columns(0)
        (loglevel,1)
    })

    val rdd3 = rdd2.reduceByKey( (x,y) => x+y )
    // saving the result in a folder
    // rdd3.saveAsTextFile("path")
    rdd3.collect().foreach(println)

    // chained up
    // sc.parallelize(mylist).map( x => x.split(":")(0),1).reduceByKey(_+_).collect().foreach(println)


}