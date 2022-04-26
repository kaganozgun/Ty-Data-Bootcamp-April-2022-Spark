import model.{Movie, Rating, Tag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import parser.ParseCsvData
import task.{exercise1Rdd, exercise2Rdd, exercise3Rdd, exercise4Rdd, exercise5Rdd, exercise6Rdd}

object mainRdd {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("SparkExercise").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val movieLines = sc.textFile("data/movies.csv")
    val ratingLines = sc.textFile("data/ratings.csv")
    val tagLines = sc.textFile("data/tags.csv")

    val movies: RDD[Movie] = movieLines.filter(line => line != "movieId,title,genres").map{
      line => ParseCsvData.parseMovie(line)
    }
    val ratings: RDD[Rating] = ratingLines.filter(line => line != "userId,movieId,rating,timestamp").map{
      line => ParseCsvData.parseRating(line)
    }
    val tags: RDD[Tag] = tagLines.filter(line => line != "userId,movieId,tag,timestamp").map{
      line => ParseCsvData.parseTag(line)
    }

    //Exercise 1
    print("Exercise 1 RDD:\n")
    exercise1Rdd(movies).collect().foreach(println)

    //Exercise 2
    print("Exercise 2 RDD:\n")
    exercise2Rdd(movies, ratings).collect().foreach(println)

    //Exercise 3
    print("Exercise 3 RDD:\n")
    exercise3Rdd(movies).collect().foreach(println)

    //Exercise 4
    print("Exercise 4 RDD:\n")
    exercise4Rdd(movies, ratings, tags).sortBy(_._1, ascending = true).collect().foreach(println)

    //Exercise 5
    print("Exercise 5 RDD:\n")
    println(exercise5Rdd(movies, tags, ratings))

    //Exercise 6
    print("Exercise 6 RDD:\n")
    exercise6Rdd(movies, ratings, tags).collect().foreach(println)

  }

}
