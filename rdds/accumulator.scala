object reload extends App{

    val sc = new SparkContext(local[*], "accumalator")
    val input = sc.textFile("c:\\Users\\pp255070\\OneDrive - Teradata\\Documents\\sample.txt")

    val myaccum = sc.longAccumulator("blank lines accumalator")  // long is data type of accumalator, it is at cluster level that is why sc

    input.foreach(x => if (x=="") myaccum.add(1))

    myaccum.value
}