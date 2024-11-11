//saving result , filtering

//sc.defaultPararllelism  tells us about default parallelism
//res5: Int = 20

//sc.defaultMinPartitions  tells us min number of partitions
//res5: Int = 2

//input.getNumPartitions  
//res5: Int = 2

object customer extends App{

    val sc = new SparkContext(local[*], "customer")
    val input = sc.textFile("c:\\Users\\pp255070\\Downloads\\customerorders.csv")

    val required = input.map( x => ((x.split(",")(0), x.split(",")(2).toFloat)))
    val aggregated = required.reduceByKey( (x,y) => x+y)
    val sorted = aggregated.sortBy( x => x._2)
    val result = sorted.collect       // this is not rdd as it is not distributed others are rdds as they are distributed
    //result.saveAsTextfile("path to folder")

    //val filterd =  aggregated.filter( x => x._2 > 5000)
    // val double = filterd.map( x => x._2 *2)
    //double.collect.foreach(println)
    
    // println(double.count)

}