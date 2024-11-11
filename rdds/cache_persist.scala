import org.apache.spark.storage.StorageLevel  // this needed to be imported for storage level

object customer extends App{

    val sc = new SparkContext(local[*], "customer")
    val input = sc.textFile("c:\\Users\\pp255070\\Downloads\\customerorders.csv")

    val required = input.map( x => ((x.split(",")(0), x.split(",")(2).toFloat)))
    val aggregated = required.reduceByKey( (x,y) => x+y)
    val filterd =  aggregated.filter( x => x._2 > 5000)
    val double = filterd.map( x => x._2 *2).cache()// there is nothing like uncache, instead there is unpersist()
    //val double = filterd.map( x => x._2 *2).persist(StorageLevel.MEMORY_ONLY)
    double.collect.foreach(println)
    println(double.count)

    double.toDebugString()  // it will give lineage graph, read it from bottom

    //val sorted = aggregated.sortBy( x => x._2)
    //val result = sorted.collect       // this is not rdd as it is not distributed others are rdds as they are distributed
    //result.saveAsTextfile("path to folder")

    //val filterd =  aggregated.filter( x => x._2 > 5000)
    // val double = filterd.map( x => x._2 *2)
    //double.collect.foreach(println)
    
    // println(double.count)
}