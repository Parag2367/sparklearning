import org.apache.spark.SparkContext
import org.apache.log4j.logger
import org.apache.log4j.level


object week10_1 extends App{


    val sc = new SparkContext("loacl[*]","chapters")

    val chapters = sc.textFile("c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\assignment\\week10\\chapters-201108-004545.csv")

    val chaptersRdd = chapters.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))

    val total = chaptersRdd.map( x => (x._2,1)).reduceByKey( (x,y) => (x+y))

    total.collect.foreach(println)
}