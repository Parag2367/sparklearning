import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object read extends App {

//another way of creating spark session
    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","my app")
    sparkconf.set("spark.master","local[2]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()   

    val ordersDf = spark.read.csv("C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.csv")

    val orders = spark.read  // with more options
    .option("header",true)
    .option("inferSchema",true)   // we should not use infer schema as we never know
    .csv("C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.csv")


    // Programatic approach to give schema we can rename column as well
    val orderschema = StructType(List
    (StructField ("order_id", IntegerType,false)),//false states that nulls are not allowed
    (StructField ("orderdate", TimestampType)),
    (StructField("order_customer_id" , IntegerType)),
    (StructField ("status", StringType)))


    // DDL string approach to define schema we can rename column as well
    val ordersDDLSchema = "orderid int, orderdate timestamp, custid int, status string"


    // industry standard way of writing read
    val ordersStandard = spark.read
    .format("csv")
    .option("header" , true)
    //.option("inferSchema" , true)
    //.schema(orderschema) //we can do it like this also by giving only schema
    .schema(ordersDDLSchema)
    .option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.csv")
    .load

    ordersStandard.show()

    ordersStandard.show(false)   // it will not truncate the records or value


    val groupped = orders.repartition(4)
    .where("order_customer_id > 1000")
    .select("order_id" , "order_customer_id")
    .groupBy("order_customer_id").count()

    groupped.show()

    //ordersDf.show()  // by default it shows 20 rows

    //ordersDf.printSchema()   //it will print schema


    //json file

    val ordersStandard = spark.read
    .format("json")
    .option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.json")
    .load

    // handling malformed records in files

    val ordersStandard = spark.read
    .format("json")
    .option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\orders-201019-002101.json")
    //.option("mode","PERMISSIVE") this is by default we dont have to mention
    .option("mode","DROPMALFORMED")
    //.option("mode","FAILFAST")  another way of read mode
    .load

    val users = spark.read
    //.format("parquet") // by default this will read it no need to mention format also
    .option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\users-201019-002101.parquet")
    .load

    users.show()



}
