import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.Window

object week12_windowAgg extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name" , "invoice")
    sparkconf.set("spark.master" , "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    val invoice = spark.read.
    format("csv").
    option("path" , "c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\windowdata-201025-223502.csv").
    option("header", true).
    option("inferSchema" , "true").
    load()

    val wind = Window.partitionBy("country").  // note: wheneever we use window agg, we have to define these three things, partiton,order,window
    orderBy("weeknum").
    rowsBetween(Window.unboundedPreceding , Window.currentRow)

    val myDf = invoice.withColumn("RunningTotal" , sum("invoicevalue").over(wind))

    myDf.show()


}