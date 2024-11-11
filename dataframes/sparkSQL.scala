import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object sparkSQL extends App {
    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()


    val ordersDf = spark.read.
                format("csv").
                option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\orders-201025-223502.csv").
                option("header",true).
                option("inferSchema", true).
                load()


    ordersDf.createOrReplaceTempView("orders")

    val resultdf = spark.sql(" select order_status , count(*) as status_count from orders group by order_status order by status_count")
    val resultsdf = spark.sql(" select order_customer_id , count(*) as order_count from orders where order_status = 'CLOSED' group by order_customer_id order by order_count")

    resultdf.show

}
