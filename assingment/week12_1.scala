import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object week12_1 extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name" , "invoice")
    sparkconf.set("spark.master" , "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()


    val employee = spark.read.format("json").option("path" ,"C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\emp.json").load()

    val dept = spark.read.format("json").option("path" ,"C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\dept.json").load()

    val dept_new = dept.withColumnRenamed("deptid","department_id")

    val joincondition = dept_new.col("department_id") === employee.col("deptid")
    val jointype = "left"

    val joined = dept_new.join( employee , joincondition ,jointype )

    //Use first function so as to get other columns also along with aggregatedcolumns
    
    joined.groupBy("department_id").agg(count("empname").as("empcount"),first("deptName").as("deptName")).dropDuplicates("deptName").show()


    // another way
    joined.createOrReplaceTempView("details")

    val summary = spark.sql(" select department_id, distinct(deptName) , sum(empname) as empcount from details group by department_id ")

}