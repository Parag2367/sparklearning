import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12 extends App {


    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()


    val mylist = List ((1,"2023-04-05",2345,"COMPLETED"),
    (2,"2022-04-05",3476,"COMPLETED"),
    (3,"2021-09-12",2255,"PENDING"),
    (4,"2020-09-09",4477,"CLOSED"))

    val mylist = List ((1,"2023-04-05",2345,"COMPLETED"),
    (2,"2022-04-05",2345,"COMPLETED"),
    (3,"2023-04-05",2255,"PENDING"),
    (4,"2020-09-09",4477,"CLOSED"))


    import spark.implicits._

    val ordersDf = spark.createDataFrame(mylist).
    toDF("order_id","date","cust_id","status")

    val newDf = ordersDf.withColumn("date", unix_timestamp(col("date").
            cast(DateType))).
            //withColumn("new_column", lit()) // we can use lit function to pass any constant value of any data type
            //.filter(ordersDf.order_id > lit(25))
            withColumn("newId" , monotoniaclly_increasing_id). // we can use with column to add new column as well as manipulate existing column name, data type
            dropDuplicates("date" , "cust_id").  // both column should have duplicate value on same row , or you can simply drop using one column
            drop("order_id").
            sort("date")

    




}