import org.apache.spark.SparkContext
import org.apache.log4j.logger
import org.apache.log4j.level


object broadcast extends App{

    def loadBoringWords() : Set[String] = {

        var boringWords : Set[String] = Set()   // based on requirements data type changes like array,list,map
        val lines = Source.fromFile("path").getLines()
        for (line <- lines){
            boringwords += line
        }
        boringWords
    } // this whole function is running on driver locally

    val sc = new SparkContext(local[*] , "reload")
    val input = sc.textfile("file_path")
    // big data course , 24.06
    // big data training , 29.06
    //.....

    //broadcast variable 
    var nameSet = sc.broadcast(loadBoringWords)  // this is getting broadcasted on every machine

    val required = input.map( x => (x.split(",")(10).toFloat, x.split(",")(0)))
    // 24.06 , big data course
    // 35.06 , big data training
    //.....

    val mapped = required.flatMapValues( x => x.split(" "))
    //(24.06 , big)
    //(24.06 , data)
    //(24.06 , course)
    //...

    val check = mapped.map(x => x._2.toLowerCase(), x._1)
    //(big, 24.06)
    //(data, 24.06)
    //(data, 35.06)
    //(big, 35.06)
    //....

    //filtering using broadcast variable
    val filtered = check.filter(x => !nameSet.value(x._1))
    // all boring words like in , for ,etc which are in file are ignored

    val total = filtered.reduceByKey(x,y => (x+y))
    //(big , 59.12)
    //(data , 59.12)
    //...
    val sorted = total.sortBy( x => x._2,false)
    sorted.take(20).foreach(println)

}