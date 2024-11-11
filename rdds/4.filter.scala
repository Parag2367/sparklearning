object reload extends App{

    val sc = new SparkContext(local[*] , "reload")
    val input = sc.textfile("file_path")
    // big data course , 24.06
    // big data training , 29.06
    //.....

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
    val total = check.reduceByKey(x,y => (x+y))
    //(big , 59.12)
    //(data , 59.12)
    //...
    val sorted = total.sortBy( x =x._2,false)
    sorted.take(20).foreach(println)

}