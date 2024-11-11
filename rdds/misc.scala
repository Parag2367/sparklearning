// how to run a jar file or file project from command line, in industry we do it like this
// following changes neede to be done to your daily code

object misc extends App {

    def main( args: Array[String]) = { // created this so that we can pass arguments while running

        val sc = new SparkContext() // removed local[*]

        val rdd1 = sc.textFile( args(0) ) // instead of passing file path , we passed the argument(0), we can pass many like this args(1), args(2), etc

        val rdd2 = rdd1.map( x => (x.split(":")(0), x.split(":")(1))).groupByKey.map( x => ( x._1, x._2.size))

    }

}

// how to run a file from command line

// spark-submit --class misc --master yarn --deploy-mode cluster --executor-memory 3G --num-executors 4 rdds.jar file_path/file_name