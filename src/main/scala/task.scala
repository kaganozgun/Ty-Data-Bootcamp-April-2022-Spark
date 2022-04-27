import model.{Movie, Rating, Tag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import org.apache.spark.sql.functions.{avg, col, lit, when}

object task {

  def exercise1Rdd(movies: RDD[Movie]): RDD[(Int, String, Int)] = {
    val result = movies.flatMap(v=> v.genres.map(g => (v.year, g))).keyBy{ case(c1, c2) => (c1.getOrElse(-1),c2)}
      .mapValues(_ => 1).reduceByKey(_+_).sortBy(a => (a._1._1, a._2), ascending = false)
    result.map(a => (a._1._1, a._1._2, a._2))
  }

  def exercise1Df(moviesYearExplodedGenre: DataFrame): Dataset[Row] = {
    moviesYearExplodedGenre.groupBy(col("year"), col("genre")).count().orderBy(col("year"))
  }

  def exercise2Rdd(movies: RDD[Movie], ratings: RDD[Rating]): RDD[(Int, Double)] = {
    val avgRatingByMovie = ratings.map(k => (k.movieId, k.rating)).groupByKey
      .mapValues{iterator => iterator.sum / iterator.size}
    val movieIdYearMap = movies.map(k => (k.id, k.year.getOrElse(-1)))
    avgRatingByMovie.join(movieIdYearMap).map(k => (k._2._2, k._2._1)).groupByKey
      .mapValues{ it => it.sum / it.size}.sortBy(s => s._2, ascending = false)
  }

  def exercise2Df(moviesYearExplodedGenre: DataFrame, ratings: DataFrame) = {
    val ratingsRenamed = ratings.withColumnRenamed("movieId", "r_movieId")
    ratingsRenamed
      .join(moviesYearExplodedGenre,
        ratingsRenamed("r_movieId") === moviesYearExplodedGenre("movieId"), "inner")
      .groupBy(col("year")).agg(avg(col("rating")).alias("avg_rating"))
      .orderBy(col("avg_rating").desc)
  }

  def exercise3Rdd(movies: RDD[Movie]) = {
    val genreMovieCnt = movies.flatMap(v=> v.genres.map(g => (v.id, g))).keyBy{ case(c1, c2) => (c2)}
      .mapValues(_ => 1).reduceByKey(_+_).map(k => (k._1, k._2))

    val aa = movies.flatMap(v=> v.genres.map(g => (v.year, g))).keyBy{ case (c1,c2) => (c1.getOrElse(-1), c2)}
      .mapValues(_ => 1).reduceByKey(_+_).map( k => (k._2, k._1))

    val bb = aa.groupBy(_._2._1).mapValues(l => l.reduce((a,b) => if(a._1 < b._1) a else b))
    val cc = bb.map(k => (k._2._2._2, k))

    val totalMovieCnt = movies.collect().length

    cc.join(genreMovieCnt).map(k => (k._2._1._1, k._1, k._2._1._2._1, k._2._2, totalMovieCnt)).sortBy(_._1)
  }

  def exercise3Df(movies: DataFrame, moviesYearExplodedGenre: DataFrame) = {
    val genreMovieCnt = moviesYearExplodedGenre.groupBy(col("genre")).count()
    val totalMovieCnt = movies.count()

    val genreMovieCntTotalMovieCnt = genreMovieCnt.withColumn("total_movie_cnt", lit(totalMovieCnt))
    val genreMovieYear = moviesYearExplodedGenre.groupBy(col("year"), col("genre")).count()

    val win = Window.partitionBy(col("year"))
    val minMovieCntByYearAndGenre = genreMovieYear.groupBy(col("year"), col("genre"))
      .agg(functions.min(col("count")).as("count_by_year"))
      .withColumn("min_val", functions.min(col("count_by_year"))
        .over(win.orderBy(col("count_by_year"))))
      .filter(col("count_by_year") === col("min_val"))
      .select(col("year"), col("genre").as("genre_name"), col("count_by_year"))

    minMovieCntByYearAndGenre.join(genreMovieCntTotalMovieCnt,
      minMovieCntByYearAndGenre("genre_name") === genreMovieCntTotalMovieCnt("genre"), "inner")
      .select(col("year"), col("genre_name"), col("count_by_year"),
        col("count").as("count_by_genre"), col("total_movie_cnt"))
      .orderBy(col("year").desc)
  }

  def exercise4Rdd(movies: RDD[Movie], ratings : RDD[Rating], tags: RDD[Tag]): RDD[(String, Int, Int, Int)] = {
    val tagMap = tags.map(t => ((t.userId.toLong, t.movieId), t))
    val ratingMap = ratings.map(r => ((r.userId, r.movieId), r))
    val movieMap = movies.map(m => (m.id, m))

    val voteAndTag = tagMap.fullOuterJoin(ratingMap)

    val userGaveVoteAndTag = voteAndTag.filter(f => f._2._2.getOrElse(-1) != -1 && f._2._1.getOrElse(-1) != -1)
      .map(m => (m._1._2, m))

    val userGaveVoteAndTagWithMovieGenre = userGaveVoteAndTag.join(movieMap)
      .flatMap(v=> v._2._2.genres.map(g => (v._2._2.id, v._2._2.year, g)))

    val userGaveTag = tagMap.map(m => (m._1._2, m)).join(movieMap)
      .flatMap(v=> v._2._2.genres.map(g => (v._2._2.id, v._2._2.year, g)))

    val userGaveTagCntGenreYear = userGaveTag.groupBy(g => (g._3, g._2.getOrElse(-1))).mapValues(it => it.size)

    val userGaveTagAndRatingCntGenreYear = userGaveVoteAndTagWithMovieGenre
      .groupBy(g => (g._3, g._2.getOrElse(-1))).mapValues(it => it.size)

    val genreYearAndNumbers = userGaveTagCntGenreYear.join(userGaveTagAndRatingCntGenreYear)
      .map(m => (m._1, m._2._2, m._2._1, m._2._2.toDouble/m._2._1.toDouble))

    val maxRateYearByGenre = genreYearAndNumbers.groupBy(_._1._1)
      .mapValues( l => l.reduce((a,b) => if(a._4 > b._4) a else b)).map(m => (m._1, m._2._1._2))

    val totalVoteAndTagCntByGenre = genreYearAndNumbers.map(m => (m._1._1, (m._2, m._3))).groupByKey
      .mapValues(it => (it.map(a => a._1).sum, it.map(p => p._2).sum))

    totalVoteAndTagCntByGenre.join(maxRateYearByGenre).map(m => (m._1, m._2._1._1, m._2._1._2, m._2._2))
  }

  def exercise4Df(moviesYearExplodedGenre: DataFrame, tags: DataFrame, ratings: DataFrame): DataFrame  = {
    val tagsRenamed = tags.withColumnRenamed("movieId", "tag_movieId")
      .withColumnRenamed("userId", "tag_userId")
    val voteAndTagJoinCondition = {tagsRenamed("tag_userId") === ratings("userId") && tagsRenamed("tag_movieId") === ratings("movieId")}
    val voteAndTag = tagsRenamed.join(ratings, voteAndTagJoinCondition, "fullouter")
      .select(col("tag_userId"), col("userId").as("rating_userId"), col("movieId"))

    val userGiveVoteAndTag = voteAndTag.na.drop()
      .withColumnRenamed("movieId", "tag_rating_movieId")

    val userGiveVoteAndTagAndMovie = userGiveVoteAndTag
      .join(moviesYearExplodedGenre,
        userGiveVoteAndTag("tag_rating_movieId") === moviesYearExplodedGenre("movieId"), "inner")
      .groupBy(col("year"), col("genre")).count()
      .select(col("year").as("r_t_year"), col("genre").as("r_t_genre"),
        col("count").as("rating_vote_cnt"))

    val userGiveTagAndMovie = tagsRenamed
      .join(moviesYearExplodedGenre,
        tagsRenamed("tag_movieId") === moviesYearExplodedGenre("movieId"), "inner")
      .groupBy(col("year"), col("genre")).count()
      .select(col("year"), col("genre"), col("count").as("vote_cnt"))

    val joinCondition =
    {userGiveVoteAndTagAndMovie("r_t_year") === userGiveTagAndMovie("year") && userGiveVoteAndTagAndMovie("r_t_genre") === userGiveTagAndMovie("genre")}
    val joinedYearGenreNumbers = userGiveVoteAndTagAndMovie.join(userGiveTagAndMovie, joinCondition, "inner")
      .select(col("year"), col("genre"),
        col("rating_vote_cnt"), col("vote_cnt"),
        (col("rating_vote_cnt") / col("vote_cnt")).as("rate"))

    val win = Window.partitionBy(col("genre"))
    val minRateByYearAndGenre = joinedYearGenreNumbers
      .withColumn("max_rate", functions.max(col("rate"))
        .over(win.orderBy(col("rate").desc)))
      .filter(col("rate") === col("max_rate"))
      .withColumn("max_vote_cnt", functions.max(col("vote_cnt"))
        .over(win.orderBy(col("vote_cnt").desc)))
      .filter(col("vote_cnt") === col("max_vote_cnt"))
      .select(col("year").as("year_with_max_rate"),
        col("genre").as("genre_with_max_rate"))

    val genreTotalVoteRating = joinedYearGenreNumbers.groupBy("genre")
      .agg(functions.sum("rating_vote_cnt").as("rating_vote_cnt"),
        functions.sum("vote_cnt").as("vote_cnt"))

    genreTotalVoteRating.join(minRateByYearAndGenre,
      joinedYearGenreNumbers("genre") === minRateByYearAndGenre("genre_with_max_rate") ,
      "inner").select("genre", "rating_vote_cnt", "vote_cnt", "year_with_max_rate")
  }

  def exercise5Rdd(movies: RDD[Movie], tags: RDD[Tag], ratings: RDD[Rating])
  : (Int, (String, Int, Double), (String, Int, Double)) ={
    val userWithMaxTag = tags.groupBy(_.userId).mapValues(it => it.size).reduce((a,b) => if (a._2 > b._2) a else b)
    val userId = userWithMaxTag._1
    val voteCnt = userWithMaxTag._2

    val movieMap = movies.map(i => (i.id, i))
    val genreYearRating = ratings.filter(_.userId == userId).map(m => (m.movieId, m)).join(movieMap)
      .flatMap(v=> v._2._2.genres.map(g => (g, v._2._2.year.getOrElse(-1), v._2._1.rating)))

    val genreYearAvgRating = genreYearRating.groupBy(g => (g._1, g._2))
      .mapValues(v => v.map(_._3).sum / v.map(_._3).size.toDouble)
    val minRatedGenre = genreYearAvgRating.map(m => (m._1._1, m._1._2, m._2)).reduce((a,b) => if(a._3 < b._3) a else b)
    val maxRatedGenre = genreYearAvgRating.map(m => (m._1._1, m._1._2, m._2)).reduce((a,b) => if(a._3 > b._3) a else b)

    (voteCnt, minRatedGenre, maxRatedGenre)
  }

  def exercise5Df(moviesYearExplodedGenre: DataFrame, tags: DataFrame, ratings: DataFrame):
    (Any, (Any, Any, Any), (Any, Any, Any))= {
    val userTagCnt = tags.groupBy("userId").count()

    val maxCnt = userTagCnt.agg(functions.max("count").as("max_tag_cnt")).first().get(0)
    val maxCntUserId = userTagCnt.filter(col("count") === maxCnt).first().get(0)

    val ratingsOfMaxCntUser = ratings.filter(col("userId") === maxCntUserId)
      .select(col("movieId").as("rating_movieId"), col("rating"))

    val ratingsOfMaxCntUserMovieDetails = ratingsOfMaxCntUser.join(moviesYearExplodedGenre,
      ratingsOfMaxCntUser("rating_movieId") === moviesYearExplodedGenre("movieId"), "inner")

    val yearGenreAvgRating = ratingsOfMaxCntUserMovieDetails.groupBy(col("year"), col("genre"))
      .agg(functions.avg("rating").as("avg_rating"))

    val minRatedGenre = yearGenreAvgRating.sort(col("avg_rating").asc).first()
    val maxRatedGenre = yearGenreAvgRating.sort(col("avg_rating").desc).first()

    (maxCnt, (minRatedGenre.get(0), minRatedGenre.get(1), minRatedGenre.get(2)), (maxRatedGenre.get(0),
      maxRatedGenre.get(1), maxRatedGenre.get(2)))
  }

  def exercise6Rdd(movies: RDD[Movie], ratings: RDD[Rating], tags: RDD[Tag]): RDD[(String, String)]={
    val movieIdMinTagTs = tags.map(i => (i.movieId, i.timestamp)).groupByKey().mapValues(it => it.min)
    val movieIdMinRatingTs = ratings.map(i => (i.movieId, i.timestamp)).groupByKey().mapValues(it => it.min)

    val movieMinTagMinRating = movies.flatMap(k => k.genres.map(g => (k.id, g)))
      .fullOuterJoin(movieIdMinTagTs).fullOuterJoin(movieIdMinRatingTs)
      .map(m => (m._2._1.map(p => p._1.getOrElse("-")).getOrElse("-"),
        m._2._1.map(p => p._2.getOrElse(Long.MaxValue)).getOrElse(Long.MaxValue),
        m._2._2.getOrElse(Long.MaxValue)))

    // If tag first put 1 else rating first put -1
    movieMinTagMinRating.map(a => if(a._2 < a._3) (a._1, 1) else (a._1, -1)).groupBy(_._1)
      .mapValues(it => it.map(m => m._2).sum).map(p => if (p._2 < 0) (p._1, "Rating First") else (p._1, "Tag First"))
  }

  def exercise6Df(moviesYearExplodedGenre: DataFrame, tags: DataFrame, ratings: DataFrame) = {
    val movieIdMinTagTs = tags.groupBy("movieId")
      .agg(functions.min("timestamp").as("min_tag_ts"))
      .select(col("movieId").as("min_tag_m_id"), col("min_tag_ts"))

    val movieIdMinRatingTs = ratings.groupBy("movieId")
      .agg(functions.min("timestamp").as("min_rating_ts"))
      .select(col("movieId").as("min_rating_m_id"), col("min_rating_ts"))

    val movieMinTag = moviesYearExplodedGenre
      .join(movieIdMinTagTs, moviesYearExplodedGenre("movieId") === movieIdMinTagTs("min_tag_m_id"),
        "outer").select("movieId", "year", "genre", "min_tag_ts")
      .na.fill(Int.MaxValue)

    val movieMinRating = moviesYearExplodedGenre
      .join(movieIdMinRatingTs, moviesYearExplodedGenre("movieId") === movieIdMinRatingTs("min_rating_m_id"),
        "outer").select(col("movieId").as("r_movieId"),
      col("year").as("r_year"), col("genre").as("r_genre"),
      col("min_rating_ts")).na.fill(Int.MaxValue)

    val movieMinTagMinRating = movieMinRating
      .join(movieMinTag,
        {movieMinRating("r_movieId") === movieMinTag("movieId") && movieMinRating("r_genre") === movieMinTag("genre")},
        "outer").na.fill(Int.MaxValue).select("year", "genre", "min_rating_ts", "min_tag_ts")
      .withColumn("label",
        when(col("min_rating_ts") < col("min_tag_ts"), "Rating First")
          .otherwise("Tag First"))

    val genreTagCnt = movieMinTagMinRating.groupBy(col("genre"), col("label")).count()
    val tagFirstWithCountByGenre = genreTagCnt.filter(col("label") === "Tag First")
      .select(col("genre").as("t_genre"), col("count").as("tagFirstCnt"))
    val ratingFirstWithCountByGenre = genreTagCnt.filter(col("label") === "Rating First")
      .select(col("genre").as("r_genre"), col("label"), col("count").as("ratingFirstCnt"))

    ratingFirstWithCountByGenre
      .join(tagFirstWithCountByGenre, ratingFirstWithCountByGenre("r_genre") === tagFirstWithCountByGenre("t_genre"),
        "inner")
      .withColumn("label",
        when(col("ratingFirstCnt") > col("tagFirstCnt"), "Rating First")
          .otherwise("Tag First")).select(col("r_genre").as("genre"), col("label"))
  }


}
