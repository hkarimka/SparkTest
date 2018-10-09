package Practice2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TestTask {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //initialize spark configurations
    val conf = new SparkConf()
    conf.setAppName("words-count-example")
    conf.setMaster("local[2]")
    //SparkContext
    val sc = new SparkContext(conf)
    val inputFile = getClass.getResource("input.txt").getPath

    /*Calculate the sum of the numbers in each row of file*/
    //read file
    val fileRDD = sc.textFile(inputFile)
    val rowsRDD = fileRDD.map(_.split(";"))

    val idRDD = rowsRDD.map(s => (s(1), 1))
    val countOfHeroRDD = idRDD.reduceByKey(_+_)
    println("Count of each Hero:")
    countOfHeroRDD.foreach(print)

    println("\n")

    val killsRDD = rowsRDD.map(s => (s(1), s(2).toInt))
    val countOfKills = killsRDD.reduceByKey(_+_)
    println("Kills of each Hero:")
    countOfKills.foreach(print)
  }
}
