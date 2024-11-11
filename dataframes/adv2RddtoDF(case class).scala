import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

case class Logging( level: String , datetime: String)

def mapper(line: String) = {
    val fields = line.split(',')

    val logging:Logging = Logging( fields(0), fields(1))
    logging
}

def main (args : Array[String]) = {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    import spark.implicits._

    val mylist = List("WARN,2016-12-31 06:19:32",
    "FATAL,2016-12-31 07:09:23",
    "WARN.2016-12-31 09:09:33",
    "INFO,2015-4-21 07:09:23",
    "FATAL,2015-4-21 06:19:32")

    val rdd1 = spark.sparkContext.parallelize(mylist)

    val rdd2 = rdd1.map(mapper)

    val df1 = rdd2.toDF()

    df1.createOrReplaceTempView("logging_table")

    //warn , [2016-12-31 06:19:32,2016-12-31 09:09:33]
    //fatal, [2016-12-31 07:09:23,2015-4-21 06:19:32]
    //info, [2015-4-21 07:09:23]   // to get this we are using collect_list

    spark.sql( "select level , collect_list(datetime) from logging_table group by level order by level").show(false)

    // count of datetime entry

    spark.sql( "select level , count(datetime) from logging_table group by level order by level").show(false)

    val df2 = spark.sql( "select level , date_format(datetime, 'MMMM') as month from logging table")

    df2.createOrReplaceTempView("new_logging_table")

    spark.sql("select level , month , count(1) from new_logging_table group by level , month").show

    val df3 = spark.read.
                format("csv").
                option("header" ,true).
                option("path","c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\biglog").
                load

    df3.createOrReplaceTempView("big_logging_table")

    spark.sql("select level , date_format(datetime ,'MMMM') as month , count(1) from big_logging_table group by level, month ")

    val result1 = spark.sql(""" select level , date_format(datetime , 'MMMM') as month , cast(first(date_format(datetime 'M')) as int ) as month_number , count(1) 
    from big_logging_table group by level , month order by month_num""")
    // why we added month_number as it was sorting by month in alphabatically way so we added month number so that we can sort in a better way
    // note: in sql when you are grouping on few of the slect columns then others should be aggregated look at above example

    val result2  = result1.drop("month_number")


    // another way to solve above question using Pivot

    val result1 = spark.sql(""" select level , date_format(datetime , 'MMMM') as month , cast(date_format(datetime 'M') as int ) as month_number
    from big_logging_tabl""").groupBy("level").pivot("month_number").count()

    result1.show()

    // internally system will run query for distinct month as to give utput, but months are not gonna change ever so more optimized way is followin


    val columns = List("January" ,"February" ,"March","April" ,"May" ,"June","July","August","September","October","November","December")

    val result1 = spark.sql(""" select level , date_format(datetime , 'MMMM') as month
    from big_logging_tabl""").groupBy("level").pivot("month", columns).count()

    // in pivot ("month" , columns) month is month from data and columns is the value that we have told the system to stick with.







    

}