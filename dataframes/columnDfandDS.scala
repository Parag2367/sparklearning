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

// column string way

    ordersDf.select("order_id_id", "order_status").show()

// column object way

      //did this to use $ , '  to specify columns
    ordersDf.select(col("order_id"), column("order_status"), $"order_date", 'order_cutomer_id).show()

// added an expr to convert expr to object , but we can only use column object in this case
    ordersDf.select(col("order_id"), expr("concat(order_status,'_STATUS')")).show()

// better way of achieing the above is 

    ordersDf.selectExpr("order_id", "concat(order_status,'_STATUS')").show()





}