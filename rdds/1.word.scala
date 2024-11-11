// object WordCount {

//     def main ( args: Array[String]) {

//         /////// you can write code inside this 

//     }

// }

 // you can use it this way if you want to define main
 // otherwise you can use below one which more clearer which uses extends App.
// local[*] shows running on local and use all cores

// on terminal we used to use sc (spark context) but here we have to create are own spark context
object WordCount extends App{

    val sc = new SparkContext(local[*] , "wordcount")
    val input = sc.textFile("file_path")
// big data course in banglore
//big data trending 
//convert above into
//big
//data
//..
//..
//to get this we will use Flatmap transformation on space
    val word = input.flatMap( x => x.split(" "))
    //val word = input.flatMap( _.split(" "))
//(big,1)
//(BIG,1)
//but they are same words so we can use following
    val wordlower = word.map(x => x.toLowerCase())
    //val wordlower = word.map(_.toLowerCase())

    val wordmap = word.map( x => (x,1))
    //val wordmap = word.map( _,1))
//(big,1)
//(data,1)
//(key,value)
//((a,b) => a+b it will sum the values)
    val wordcount = wordmap.reduceByKey((a,b) => a+b)
//reduce by key works on two rows
// it will sum values of these two rows with same key
    wordcount.collect.foreach(println)

//Chaining of above functions
    val chainresult = input.flatMap( x => x.split(" ")).
    map(x => x.toLowerCase()).
    map( x => (x,1)).
    reduceByKey((a,b) => a+b).
    collect


//// If we want to find top 10 words which are mostly used
// we can use sort for that, sort the values but sortByValue is not available , we have sortByKey available so we will use that as following

    val reversed = wordcount.map(x => (x._2, x._1))   //x._1 is a way to access first element in tuples as wordcount was tuple
    val sorted = reversed.sortByKey(false) // false is for descending order , by default is ascending order
    val finaly = sorted.map(x => (x._2, x._1))
    finaly.collect.foreach(println)

    for (a <- finaly) {
        word = finaly._1
        count = finaly._2
        print(s"$word:$count")
}
}


// A better way of or achieveing above is using sortBy

val finaly = wordcount.sortBy( x => x._2,false)

}

