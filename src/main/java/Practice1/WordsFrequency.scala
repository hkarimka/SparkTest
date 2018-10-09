package Practice1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordsFrequency {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //initialize spark configurations
    val conf = new SparkConf()
    conf.setAppName("words-count-example")
    conf.setMaster("local[2]")
    //SparkContext
    val sc = new SparkContext(conf)
    val inputFile = getClass.getResource("text.csv").getPath

    //read file
    val fileRDD = sc.textFile(inputFile)
    //create RDD[String] collection with all words
    val allWordsRDD = fileRDD.flatMap(s => s.split(" "))
    //create RDD[(String), (Int)] map with all words and 1
    val wordsPairsMapRDD = allWordsRDD.map(s => (s, 1))
    //merge wordsPairs which are the same and count accumulatedValue + currentValue
    val wordsFreqMapRDD = wordsPairsMapRDD.reduceByKey { case (a, b) => a + b }

    wordsFreqMapRDD.foreach(println)
  }
}
