import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12_2 extends App {

    case class ratings (userid: Int, movieid: Int, rating: Int, timetamp: String)  // creating case class is necessary in some cases
    
    case class movies ( movieid:Int, moviename: String, genre: String) // creating class is necessary as it is not structured in proper way
    // we could also use Struct field , DDL string type like in read.scala program but for that data needs to have comma separated value

    def mapper (line:String) = {   // this can be done or we can achieve this like schRatings below
        val fields = line.split("::")

        val ratings_movie : ratings = ratings ( fields(0).toInt , fields(1).toInt , fields(2).toInt , fields(3))
        ratings_movie

    }

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name" , "invoice")
    sparkconf.set("spark.master" , "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    val ratingsRdd = spark.sparkContext.textFile("path")
    val moviesrdd = spark.sparkContext.textFile("path")

    import spark.implicits._
    // one way
    val schRatings = ratingsRdd.map( x => x.split("::")).map( x => ratings( x(0).toInt , x(1).toInt , x(2).toInt , x(3)))
    val ratingsdf = schRatings.toDF()

    // another way
    val rdd1 = ratingsRdd.map(mapper)
    val moviesdf = rdd1.toDF()

    // for movies
    val moviesrdd1 = moviesrdd.map( x => x.split("::")).map( x => movies( x(0).toInt, x(1), x(2)))
    val df2 = moviesrdd1.toDF()

    val transformedDf = ratingsdf.groupBy("movieid").
    agg(count("rating").as("movieviewcount"), avg("rating").as("avgMovieRating").orderBy(desc("movieviewcount")))

    val popular = transformedDf.filter("movieviewcount > 1000 and avgMovieRating > 4.5")


    spark.sql(" SET spark.sql.autoBrodcastJoinThreshold = -1")

    val joincondition = popular.col("movieid") === moviesdf.col("movieid")
    val joinType = "inner" 

    // we have used drop column method to resolve ambiguous column error as column have same name

    val finalPopular = popular.join(broadcast(moviesdf) ,joincondition, joinType).drop(popular.col("movieid")).
    orderBy(desc("avgMovieRating"))

    


}