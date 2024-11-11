import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SortAggandHashAgg extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name","adv")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

// doing it in spark-shell by using following statements    
// spark-shell --conf spark.dynamicAllocationEnabled=false --master yarn --num-executors 11

   val df1 = spark.read.format("csv").option("path","").option("header",true).option("inferSchema", true).load()

    df1.createOrReplaceTempView("order")

    spark.sql(""" select order_customer_id, date_format(order_date ,'MMMM') dt , count(1) cnt, first(date_format(order_date ,'M')) monthnum 
    from orders group by order_customer_id , dt
    order by cast(monthnum as int)
      """")

      // it took 3.9 mins to complete above query

    // now optimized way:

    spark.sql(""" select order_customer_id, date_format(order_date ,'MMMM') dt , count(1) cnt, first(cast(date_format(order_date ,'M') as int) monthnum 
    from orders group by order_customer_id , dt
    order by monthnum
      """")
      // it took 1.2 mins only, 

    spark.sql(""" select order_customer_id, date_format(order_date ,'MMMM') dt , count(1) cnt, first(cast(date_format(order_date ,'M') as int) monthnum 
    from orders group by order_customer_id , dt
    order by monthnum
      """").explain // this will give us the physical plan for the query
      
    // explaination is in notes for different time

      // why we used order_date twice so as to order by in integer , why we used first so as to aggregate the column see session spark optimization 17
}
