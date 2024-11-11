import org.apache.spark.SparkContext
import org.apache.log4j.logger
import org.apache.log4j.level


object week10_2 extends App{


    val sc = new SparkContext("loacl[*]","chapters")

    val base1 = sc.textFile("c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\assignment\\week10\\views*.csv")
    val base2 = sc.textFile("c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\assignment\\week10\\chapters-201108-004545.csv")
    val titlesDataRDD = sc.textFile("c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\assignment\\week10\\titles-201108-004545.csv").
        map( x => (x.split(",")(0).toInt, x.split(",")(1)))
    val chapters = base2.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))

    val views = base1.map( x => (x.split(",")(0).toInt,x.split(",")(1).toInt))

    val viewDistinct = views.distinct()

    val flipped = viewDistinct.map( x => (x._2 , x._1))

    val joinedRdd = flipped.join(chapters)

    val pairRdd = joinedRdd.map( x => ((x._2._1 , x._2._2),1))

    val userPerCourseViewRDD = pairRdd.reduceByKey( _+_ )

    val courseViewsCountRDD = userPerCourseViewRDD.map( x => (x._1._2,x._2))

    val newJoinedRDD = courseViewsCountRDD.join(total)  // total is in week10_1.scala

    val CourseCompletionpercentRDD = newJoinedRDD.mapValues(x => (x._1.toDouble/x._2))

    val formattedpercentageRDD = CourseCompletionpercentRDD.mapValues(x => f"$x%01.5f".toDouble)

    val scoresRDD = formattedpercentageRDD.mapValues (x => {
                if(x >= 0.9) 10l
                else if(x >= 0.5 && x < 0.9) 4l
                else if(x >= 0.25 && x < 0.5) 2l
                else 0l

                })

    val totalScorePerCourseRDD = scoresRDD.reduceByKey((V1,V2) => V1 + V2)

    val title_score_joinedRDD = totalScorePerCourseRDD.join(titlesDataRDD).map( x => (x._2._1, x._2._2))

    val popularCoursesRDD = title_score_joinedRDD.sortByKey(false)

    popularCoursesRDD.collect.foreach(println)


    // chapterId , (userId,courseId)


}