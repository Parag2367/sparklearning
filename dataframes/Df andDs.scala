import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

 
case class Ordersdata(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String) // this is how we create case class,
//this is needed to convert dataframe to dataset

object example extends App {

//another way of creating spark session
    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","my app")
    sparkconf.set("spark.master","local[2]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    val orders : Dataset[Row] = spark.read  // can also be written like this
    .option("header",true)
    .option("inferSchema",true)
    .csv("C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.csv")

    //this part creates dataset from dataframes along with case class
    import spark.implicits._
    val ordersDs = orders.as[Ordersdata]

    // val df2 = ds.toDF() -- this way we can convert a dataset to dataframe
    //val ordersRddToDs = <name_of_rdd>.toDS -this way we can convert RDD to Datasets
    //val ordersRddToDf = <name_of_rdd>.toDF  -this way we can convert RDD to Datasets

    orderDs.filter( x => x.order_id < 10)  // 

    //orders.filter( x => x.)  // not possible for dataframe as the bound at runtime not at compile time
    orders.filter("order_ids < 10").show()  // it will give error on runtime as no column of name order_ids drawback of Dataframe






}