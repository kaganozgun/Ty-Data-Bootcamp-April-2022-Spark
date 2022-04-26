import model.{Movie, Rating, Tag}
import org.apache.spark.rdd.RDD

object task {

  def exercise1Rdd(movies: RDD[Movie]): RDD[(Int, String, Int)] = {
    val result = movies.flatMap(v=> v.genres.map(g => (v.year, g))).keyBy{ case(c1, c2) => (c1.getOrElse(-1),c2)}
      .mapValues(_ => 1).reduceByKey(_+_).sortBy(a => (a._1._1, a._2), ascending = false)
    result.map(a => (a._1._1, a._1._2, a._2))
  }

  def exercise2Rdd(movies: RDD[Movie], ratings: RDD[Rating]): RDD[(Int, Double)] = {
    val avgRatingByMovie = ratings.map(k => (k.movieId, k.rating)).groupByKey
      .mapValues{iterator => iterator.sum / iterator.size}
    val movieIdYearMap = movies.map(k => (k.id, k.year.getOrElse(-1)))
    avgRatingByMovie.join(movieIdYearMap).map(k => (k._2._2, k._2._1)).groupByKey
      .mapValues{ it => it.sum / it.size}.sortBy(s => s._2, ascending = false)
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


}
