
object TopMovies extends App {

    val sc = new SparkContext("local[*]" , "Topmovies")
    val base = sc.textFile("C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\ratings-201019-002101.dat")

    val rdd1 = base.map( x => {
        val fields = x.split("::")
        (fields(1), fields(2))
        })
    val rdd2 = rdd1.mapValues( x => ((x.toFloat),1))

    val rdd3 = rdd2.reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    val filtered = rdd3.filter( x => (x._2._2 > 1000))

    val result = filtered.mapValues( x => (x._1/x._2)).filter( x => (x._2 > 4.5))

    result.collect.foreach(println)

    val moviesrdd = sc.textFile(r"C:\Users\pp255070\OneDrive - Teradata\Documents\sparklearning\movies-201019-002101.dat")

    val moviesrdd1 = moviesrdd.map( x => {
        val fields = x.split("::")
        (fields(0) , fields(1))
    })

    val joined = moviesrdd1.join(result)

    val topmovies = joined.map( x => (x._2._1))

    topmovies.collect.foreach(println)

    scala.io.StdIn.readLine()
}