import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12_DfJoin extends App {

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


    // spark.sql(" SET spark.sql.autoBroadcastJoinThreshold =-1")  this for keeping the automaticat optimization off for joins

    //val joinDf = orders.join(customers , orders.col("order_customer_id") === customers.col("customer_id"),"inner")

    val joinCondition =  orders.col("order_customer_id") === customers.col("customer_id")
    val joinType = "inner"
    val joinDf = orders.join( customers , joinCondition , joinType)

    // right join

    val joinCondition =  orders.col("order_customer_id") === customers.col("customer_id")
    val joinType = "right"
    val joinDf = orders.join( customers , joinCondition , joinType).sort("order_customer_id")

//ambiguos column example

    // Renaming column method

    val ordersNew = orders.withColumnRenamed("order_customer_id","cust_id")

    val joinCondition =  ordersNew.col("cust_id") === customers.col("customer_id")
    val joinType = "right"
    val joinDf = ordersNew.join( customers , joinCondition , joinType).
    select("order_id" , "customer_id" ,"customer_fname")

     // Drop column method

    val ordersNew = orders.withColumnRenamed("order_customer_id","cust_id")

    val joinCondition =  orders.col("cust_id") === customers.col("customer_id")
    val joinType = "right"
    val joinDf = orders.join( customers , joinCondition , joinType).
    drop(orders.col("customer_id")).
    select("order_id" , "customer_id" ,"customer_fname").
    sort("order_id")


// handling Null values: we use coalesce for that coalesce(col_name,<value_for_null>)

    val joinCondition =  orders.col("order_customer_id") === customers.col("customer_id")
    val joinType = "right"
    val joinDf = orders.join( customers , joinCondition , joinType).sort("order_customer_id").
    withColumn("order_id" ,expr("coalesce(order_id , -1)"))



}   