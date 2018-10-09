package Practice1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object NumbersOperations {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //initialize spark configurations
    val conf = new SparkConf()
    conf.setAppName("words-count-example")
    conf.setMaster("local[2]")
    //SparkContext
    val sc = new SparkContext(conf)
    val inputFile = getClass.getResource("numbers.txt").getPath

    /*Calculate the sum of the numbers in each row of file*/
    //read file
    val fileRDD = sc.textFile(inputFile)
    //create RDD[Array[String]] for array of rows
    val arrayOfRowsRDD = fileRDD.map(s => s.split(" "))
    //create RDD[Array[Int]] for array[int] of numbers of each row
    val arrayOfNumbersRDD = arrayOfRowsRDD.map(s => s.map(t => t.toInt))
    //create RDD[Int] for sum of each row by accumulating values in each arrayOfNumbersRDD[i]
    val sumOfRowsRDD = arrayOfNumbersRDD.map(t => t.reduce((a, b) => a + b))

    println("\nArray of numbers:")
    arrayOfNumbersRDD.map(s => s.mkString(" ")).foreach(println) //just for print
    println("\nSum of numbers of each row:")
    sumOfRowsRDD.map(s => s.toString + " ").foreach(print) //just for print

    /*Calculate the sum of numbers of each row, which are multiples of the 5 (number %5 == 0)*/
    val sumOf5Numbers = arrayOfNumbersRDD.map(_.filter(_ % 5 == 0)).map(_.sum)
    println("\nSum of %5 numbers of each row:")
    sumOf5Numbers.map(_.toString + " ").foreach(print) //just for print

    /*Find max and min values*/
    val maxValuesRDD = arrayOfNumbersRDD.map(_.max)
    println("\nMax values of each row:")
    maxValuesRDD.map(_.toString + " ").foreach(print)

    val minValuesRDD = arrayOfNumbersRDD.map(_.min)
    println("\nMin values of each row:")
    minValuesRDD.map(_.toString + " ").foreach(print)

    println("\nDistinct numbers of each row:")
    val distinctArrayRDD = arrayOfNumbersRDD.map(_.distinct)
    distinctArrayRDD.map(_.mkString(" ")).foreach(println)


  }

}
