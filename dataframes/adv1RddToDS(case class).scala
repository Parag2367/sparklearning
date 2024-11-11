import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

//sample data
// 1 13-03-1994     23,COMPLETED
// 2 19-09-2011     345,PENDING

object adv1 extends App {

    val myregex = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r  // did this to convert unstructed data of file to regex and try giving structure later
    case class Orders(order_id:Int, order_customer_id:Int , order_status:String) // for Dataset conversion

    def parser(line: String) = {
        line match {
            case myregex(order_id,date,order_customer_id,order_status) =>
                Orders(order_id.toInt,order_customer_id.toInt,order_status)
        }
    }

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","sparksql")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()


    val lines = spark.sparkContext.textFile("path")  // created a rdd

    import spark.implicits._  //needed to covert to dataset

    val orderDs = lines.map(parser).toDS.cache // here the unstructured data is converted to rdd and then converted to DS

    orderDs.printSchema()
    orderDs.select("order_id").show()
    orderDs.groupBy("orer_status").show()



}