import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12_simpleAgg extends App {

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

    // column object method

    invoice.select(
        count("*").as("RowCount"),
        sum("Quantity").as("TotalQuantity"),
        avg("UnitPrice").as("Average"),
        countDistinct("InvoiceNo").as("DistinctInvoice")).
        show()

    // column string method

    invoice.selectExpr(
        "count(*) as RowCount",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as Average",
        "count(Distinct(InvoiceNo)) as DistinctInvoice").
        show()

    // Spark Sql , easier way as compared to dataframne and datasets

    invoice.createOrReplaceTempView("sales")

    spark.sql(" select count(*), sum(Quantity) , avg(UnitPrice) , count(Distinct(InvoiceNo)) from sales").show()
}