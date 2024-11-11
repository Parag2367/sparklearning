import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week14_1 extends App {


    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()


    val order_schema = StructType(List(
        StructField("order_id", IntegerType),
        StructField("order_date", TimestampType),
        StructField("order_customer_id", IntegerType),
        StructField("order_status", StringType))
    )

    val orders = spark.read.format("csv").schema(order_schema).option("header", true).load()

    val cust_schema = StructType(List(
        StructField("order_item_id", IntegerType),
        StructField("order_item_order_id", IntegerType),
        StructField("order_item_product_id", IntegerType),
        StructField("order_item_quantity", IntegerType),
        StructField("order_item_subtotal", FloatType),
        StructField("order_item_price", FloatType)
    ))

    val customers = spark.read.format("csv").schema(cust_schema).option("header", true).load()

    spark.sql(" SET spark.sql.autoBroadcastJoinThreshold=-1")

    val joincondition = orders.col("order_id") === customers.col("order_item_order_id")

    val jointype = "inner"

    val finalDF = customers.join( broadcast(orders) , joincondition, jointype).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val dfSelect = finalDf.groupBy(to_date(col("order_date")).alias("date"), col("order_status").
    agg(
        round(sum("order_item_subtotal"),2).alias("total_amount"), countDistinct("order_id").alias("total_orders")).
    orderBy(
        col("date").desc,
        col("order_status"),
        col("total_amount").desc,
        col("total_orders")
    )
    )
    // SparkSql approach for achieving the same

    finalDF.createOrReplaceTempView("details")

    detailsDF = spark.sql(""" select cast(to_date(order_date) as String) as date, order_status, 
    cast(sum( order_item_subtotal) as DECIMAL (10,2) as total_amount , count(Distinct(order_id)) as total_orders from details
    group by to_date(order_date), order_status
    order by date desc, order_status, total_amount desc, total_orders
      """)


}