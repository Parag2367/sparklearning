object customer extends App{

    val sc = new SparkContext(local[*], "customer")
    val input = sc.textFile("c:\Users\pp255070\Downloads\customerorders.csv")

    val required = input.map( x => ((x.split(",")(0), x.split(",")(2).toFloat)))
    val aggregated = required.reduceByKey( (x,y) => x+y)
    val sorted = aggregated.sortBy( x => x._2)
    val result = sorted.collect       // this is not rdd as it is not distributed others are rdds as they are distributed

}