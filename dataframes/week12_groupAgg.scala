import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12_groupAgg extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name" , "invoice")
    sparkconf.set("spark.master" , "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    val invoice = spark.read.
    format("csv").
    option("path" , "c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\order_data-201025-223502.csv").
    option("header", true).
    option("inferSchema" , true).
    load()

    // Group Aggregation


    // column object method

    val summary = invoice.groupBy("Country" , "InvoiceNo").
    agg(sum("Quantity").as("TotalQuantity"),
    sum(expr("Quantity * UnitPrice")).as("InvoiceValue"))


    // column string method

    val Stringsummary = invoice.groupBy("Country" , "InvoiceNo").
    agg(expr(" sum(Quantity) as TotalQuantity"),
    expr(" sum (Quantity * UnitPrice) as InvoiceValue"))


    // Spark SQL way

    invoice.createOrReplaceTempView("Details")

    val SQLSummary = spark.sql(""" select Country, InvoiceNo, sum(Quantity) as TotalQuantity , sum(Quantity * UnitPrice) as InvoiceValue 
    from Details
    group by Country, InvoiceNo """)

}