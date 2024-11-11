import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12_DfBroadcastJoin extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name" , "invoice")
    sparkconf.set("spark.master" , "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()


    val orders = spark.read.
                format("csv").
                option("path" ,"c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\orders-201025-223502.csv").
                option("header" ,true).
                load()

    val customers = spark.read.
                format("csv").
                option("path","c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\customers-201025-223502.csv").
                option("header" ,true).
                load()

    spark.sql("SET spark.sql.autoBroadcastJoinThreshold =-1") //this for keeping the automaticat optimization off for joins

    val joinCondition =  orders.col("order_customer_id") === customers.col("customer_id")
    val joinType = "inner"


    val joinDf = orders.join(broadcast(customers) , joinCondition , joinType)

}