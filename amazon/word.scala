
object word {

    def main(args:Array[String]){

        val sc = new SparkContext() //for aws we do not mention local[*]

        val inputs = sc.textFile("s3n://path/file.txt") // we use s3n buckets

        val mapped = inputs.map( x => x.split(" "))

        val grouped = mapped.map( x => (x,1))

        val final = grouped.reduceByKey( (x,y) => (x+y))


    }
}