package Practice6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object MoviesRecommendation {
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  case class Movie(movieId: Int, movieName: String, rating: Float)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    return Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    //disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val moviesFile = "C:\\Users\\Karim\\IdeaProjects\\SparkTest\\src\\main\\resources\\movies.dat"
    val ratingsFile = "C:\\Users\\Karim\\IdeaProjects\\SparkTest\\src\\main\\resources\\ratings.dat"
    val personalRatingsFile = "C:\\Users\\Karim\\IdeaProjects\\SparkTest\\src\\main\\resources\\personalRatings.txt"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-read-csv")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    //Load my ratings
    val myRating = sparkSession.read.textFile(personalRatingsFile)
      .map(parseRating)
      .toDF()

    //Load Ratings
    val ratings = sparkSession
      .read.textFile(ratingsFile)
      .map(parseRating)
      .toDF()

    //Load Movies
    val moviesRDD = sparkSession
      .read.textFile(moviesFile).map {
      line => val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }

    //show the DataFrames
    ratings.show(10)
    myRating.show(10)

    val numRatings = ratings.distinct().count()
    val numUsers = ratings.select("userId").distinct().count()
    val numMovies = moviesRDD.count()

    // Get movies dictionary
    val movies = moviesRDD.collect.toMap
    movies.foreach(println)
    ratings.show(20)
    myRating.show(20)

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numMovies + " movies.")

    val ratingWithMyRats = ratings.union(myRating)

    // Split dataset into training and testing parts
    val Array(training, test) = ratingWithMyRats.randomSplit(Array(0.5, 0.5))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    //Get trained model
    val model = als.fit(training)

    //Evaluate Model Calculate RMSE
    val predictions = model.transform(test)
    predictions.na.drop
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

    //Get My Predictions
    val myPredictions = model.transform(myRating).na.drop

    //Show your recomendations
    val myMovies = myPredictions.map(r => Movie(r.getInt(1), movies(r.getInt(1)), r.getFloat(2))).toDF
    myMovies.show(100)
  }
}
