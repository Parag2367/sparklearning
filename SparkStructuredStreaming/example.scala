// we are using netcat utility to send words

object example extends App {

    val myconf = new SparkConf
    myconf.set("spark.app.name" , "structurestreaming")
    myconf.set("spark.master" ,"local[2]")
    myconf.set("spark.sql.shuffle.partitions", 3)// reduce the default partition from 200 to 3 to make it fast as data is small
    myconf.set("spark.streaming.stopGraceFullyOnShutdown", "true")
    myconf.set("spark.sql.streaming.schemaInference", "true") // this if for schema infer for json format or other file format, different way then spark batch processing



    val spark = SparkSession.builder().
    config(myconf).
    getOrCreate()

    // read the stream
    // Socket source : not applicable for production
    val linesDf = spark.readStream.format("socket").option("host", "localhost").option("port","4488").load()

    // File source: very popular

    val filedf = spark.readStream.format("json").option("path","C:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sparklearning\\inputfolder").
    load()



    // process the stream

    val wordsdf = linesDf.selectExpr("explode(split(value, ' ')) as word")
    val countdf = wordsdf.groupBy("word").count()

    // split(value, ' ') this will give an array which will split the lines as per ' ' : ['hello', 'hi', 'big']
    // explode will break the array and pass every element of array as a new row just like below
    // hello
    // hi
    // big

    // when we use select with dataframes we will have to give name of column
    // but when we use selectExpr it will give us the leverage to use sql query

    // write the stream

    val wordcount = countdf.writeStream.format("console").outputMode("complete").option("checkpointLocation", "checkpoint-location1")
    
    
    // example for triggering the application ( Time interval)
    val wordcount = countdf.writeStream.format("console").outputMode("complete").option("checkpointLocation", "checkpoint-location1")
    .trigger(Trigger.ProcessingTime("30 seconds")) // time interval trigger, if not mention goes with default (unspecified)
    .start() // whenever we call start it is an action



    wordcount.awaitTermination()

    //outputmode
    // complete mode: it will give all the entry from begining till end 
    // update mode: it will update and insert , if matching data is there it will update it and insert new one , and unmatching data is removed
    // append mode : it will append the entry data and old data is removed



}