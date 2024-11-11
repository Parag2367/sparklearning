import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object writeexample extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","load")
    sparkconf.set("spark.master" , "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf).
    enableHiveSupport() ///adding this to change spark table metastore to hive for onlt saveAsTable action in Bucket By for others it is not required
    .getOrCreate()

    val ordersDf = spark.read.format("csv").option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\orders-201025-223502.csv").option("inferSchema",true).option("header",true).load()

    // number of partitions
    print("ordersDf has "+ordersDf.rdd.getNumPartitions)
    // we did not not mention format for write it will save it in parquet format as it is default for structure Api

    ordersDf.write.format("json").
    mode(SaveMode.Overwrite).
    option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\write").
    save()

    // changing the number of files while writing Spark file layout
    //1. Repartition

    val ordersrep = ordersDf.repartition(4)

    ordersrep.write.format("csv").
    mode(SaveMode.Overwrite).
    option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\write").
    save()

    print("ordersrep has "+ordersrep.rdd.getNumPartitions)

    ordersrep has 4

    //2. partitionBy

    ordersDf.write.format("csv").
    mode(SaveMode.Overwrite).
    partitionBy("order_status").
    option("maxRecordsPerFile" , 2000).  // this is an setting option that we can do not necessay
    option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\write").
    save()


    //3. bucketBy
    // bucketBy(<no.of buckets>, "col_name") with this save() does not work we have to use saveAsTable for BucketBy

    spark.sql("create database if not exists retail")

    ordersDf.write.format("csv").
    mode(SaveMode.Overwrite).
    bucketBy( 4 , "order_customer_id").
    sortBy("order_customer_id").     //bucketBy works very well with sortBy, 
    option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\write").
    saveAsTable("retail.orders")
}


