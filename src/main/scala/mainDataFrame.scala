import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, element_at, explode, lit, when}
import task.{exercise1Df, exercise1Rdd, exercise2Df, exercise3Df, exercise4Df, exercise5Df, exercise6Df}

object mainDataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("SparkExercise").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val movies = spark.read.option("header", "true").option("inferSchema", "true").csv("data/movies.csv")
    val moviesYearExplodedGenre = movies
      .select(col("movieId"),
        col("title"),
        functions.split(
          element_at(functions.split(col("title"), "\\("), -1), "\\)")
          .getItem(0).cast("int").as("year"),
        explode(functions.split(col("genres"), "\\|")).as("genre"))

    val ratings = spark.read.option("header", "true").option("inferSchema", "true").csv("data/ratings.csv")
    val tags = spark.read.option("header", "true").option("inferSchema", "true").csv("data/tags.csv")

    //Exercise 1
    print("Exercise 1 DF:\n")
    exercise1Df(moviesYearExplodedGenre).show(5000, false)

    //Exercise 2
    print("Exercise 2 DF:\n")
    exercise2Df(moviesYearExplodedGenre, ratings).show()

    //Exercise 3
    print("Exercise 3 DF:\n")
    exercise3Df(movies, moviesYearExplodedGenre).show(1000, false)

    //Exercise 4
    print("Exercise 4 DF:\n")
    exercise4Df(moviesYearExplodedGenre, tags, ratings).show()

    //Exercise 5
    print("Exercise 5 DF:\n")
    println(exercise5Df(moviesYearExplodedGenre, tags, ratings))

    //Exercise 6
    print("Exercise 6 DF:\n")
    exercise6Df(moviesYearExplodedGenre, tags, ratings).show()

  }
}
