// count for 5 rating
// count for 4 rating
// count for 3 rating
// count for 2 rating
// count for 1 rating


object ratings extends App {

    val sc = new SparkContext(local[*], "ratings")
    val input = sc.textfile("filepath")

    val mapped = input.map(x =>  x.split("\t")(2))
    val rating = mapped.map( x => (x,1))

    val reducedratings = rating.reduceByKey( (x,y) => x+y)
    val result = reducedratings.collect

// instead of using map where we are using (x,1) we can use following also and the same result can be achieved
//no need of extra lines

    val result = mapped.countByValue  // this is an action and obviously is not an RDD it is a local variable

}