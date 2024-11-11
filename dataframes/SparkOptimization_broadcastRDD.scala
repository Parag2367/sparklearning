
object SparkOptimization extends App {
val sc = new SparkContext(local[*], "optimization")

val rdd1 = sc.textFile("path")

val rdd2 = rdd1.map( x => (x.split(":")(0), x.split(":")(1))) // large rdd like biglog
// (ERROR, ...)
// (WARN,...)
// (..)
//


//(...)
val a = array( // small rdd with two elements only
    ("ERROR",0),
    ("WARN",1)    
    )

val rdd3 = sc.parallelize(a)

val rdd4 = rdd2.join(rdd3)  // this is a normal join when we used this we saw it took long time

rdd4.saveAsTextfile("")

// Broadcast approach

val a = array( // small rdd with two elements only
    ("ERROR",0),
    ("WARN",1)    
    )

val bdcst = a.toMap()

val rdd3 = rdd2.map( x => ( x._1, x._2 , bdcst.value(x._1))) // this approach was way fater then above

rdd3.saveAsTextfile("name")



}