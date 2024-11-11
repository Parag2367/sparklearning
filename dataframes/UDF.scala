import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

def ageCheck(age:Int):String = {

    if (age>18) "Y" else "N"
}

object colExpr extends App {

    val sparkconf = new SparkConf
    sparkconf.set("spark.app.name", "colExpr")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
    .config(sparkconf)
    .getOrCreate()

    val detail = spark.read.
                format("csv").
                option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\Spark\\data folder\\week12\\-201025-223502.dataset1").
                option("inferSchema",true).
                load()

    val df1 = detail.toDF("name","age","city")



// column object expression udf , the func wont be register in catalog
    val parseagefunc = udf(ageCheck(_:Int):String)  // important way to register a function in Column Object way for structure APIs (df and ds)

    val df2 = df1.withColumn("adult",parseagefunc(col("age")))

// String /SQL expression UDF , the function will be registerd in catalog

    spark.udf.register("parseagefunc", ageCheck(_:Int):String )

    //spark.udf.register("parseagefunc", (x:Int) { if (x >18) "Y" else "N"} )  this way also we can do using an anoynomous func

    val df3 = df1.withColumn("adult", expr("parseagefunc(age)"))

    // listing the function

    spark.catalog.listFunctions().filter( x => x.name =="parseagefunc")  //it will give output for Sql/string expression only not for object

    // which means we can use them in our sql expression as well as it is registered

    df1.createOrReplaceTempView("peopletable")

    spark.sql("select name, age, city, parseagefunc(age) as adult from peopletable").show


}