package Practice2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQLAndDataFrames {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val jsonFile = getClass.getResource("sampletweets.json").getPath

    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-tweets")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFile)
    tweetsDF.printSchema()
    tweetsDF.createOrReplaceTempView("tweetTable")

    tweetsDF.show(25)

    //Get the most popular languages
    sparkSession.sql(
      " SELECT actor.languages, COUNT(*) as cnt FROM tweetTable GROUP BY actor.languages ORDER BY cnt DESC")
      .show(25)

    //Top devices used among all Twitter users
    sparkSession.sql(
      " SELECT generator.displayName, COUNT(*) cnt FROM tweetTable WHERE  generator.displayName IS NOT NULL" +
        " GROUP BY generator.displayName ORDER BY cnt DESC")
      .show(25)

    //Find all the tweets by user
    sparkSession.sql("SELECT COUNT(*) FROM tweetTable WHERE actor.id='id:twitter.com:983610216'").show()
  }
}
