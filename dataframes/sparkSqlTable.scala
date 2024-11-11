import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object sparkSQL extends App {
    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .enableHiveSupport() ///adding this to change spark table metastore to hive because if we terminate the metastore vanishes as metadat earlier was stored in memory
    .getOrCreate()


    val ordersDf = spark.read.
                format("csv").
                option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\orders-201025-223502.csv").
                option("header",true).
                option("inferSchema", true).
                load()

    spark.sql("create database if not exists reatil") // doing this to create own database instead of default one

    ordersDf.write.format("csv").
            mode(SaveMode.Overwrite).
            saveAsTable("reatail.orders")


    spark.catalog.listTables("retail").show() // doing this to see all tables

}