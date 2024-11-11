
val random = new scala.util.Random
val start = 1
val end = 60

val sc = new SparkContext(local[*], "salting")
val rdd1 = sc.textFile("bigLogLatest.txt")

//salting is happening here
val rdd2 = rdd1.map(x => {
var num = start + random.nextInt( (end - start) + 1 )
(x.split(":")(0) + num, x.split(":")(1))
})

val rdd3 = rdd2.groupByKey

val rdd4 = rdd3.map(x => (x._1 , x._2.size)).cache()


val rdd5 = rdd4.map(x => {
if(x._1.substring(0,4)=="WARN")
("WARN",x._2)
else
("ERROR",x._2)
})

val rdd6 = rdd5.reduceByKey(_+_).collect.foreach(println)