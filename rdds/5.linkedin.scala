// avg linkedin connections with age
//there are 4 columns we only need two columns

object connection extends App {
    def parseline(line: string) = {
        val fields = line.split(",")
        val age = fields(2).toInt
        val number = fields(3).toInt
        (age,number)
    }

    val sc = new SparkContext(local[*], "connection")
    val input = sc.textfile("filepath")

    //val mapped = input.map((x => (x.split(",")(2)), (x.split(",")(3))))  this one way as they are anonymous function we can also use defined function
// input
//(33,100)
//(33,200)
//(33,300)

    val mapped = input.map(parseline)
    val finalmapped = mapped.map( x => (x._1 , (x._2 ,1)))
    // val finalmapped = mapped.mapValues( x => (x,1))  // we can use mapValues in case keys are same and operations have to be performed on values
//(33,(100,1)
//(33,(200,1)
//(33,(300,1)
    val agg = finalmapped.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))  // x,y are values of each row not key
//(33,(600,3))
//(34,(880,4))


//(33,200)
//(34,220)
    val avg = agg.map(x => (x._1 , x._2._1/x._2._2)).sortBy( x => x._2)
    // val avg = agg.mapValues( x => x._1/x._2).sortBy( x => x._2)
    avg.collect.foreach(println)

}