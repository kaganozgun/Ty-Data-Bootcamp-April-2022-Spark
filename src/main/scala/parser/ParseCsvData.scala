package parser

import model.{Movie, Rating, Tag}

import scala.util.Try

object ParseCsvData {
  def parseMovie(line: String): Movie = {
    val splitted = line.split(",", 2)

    val id = splitted(0).toInt
    val remaining = splitted(1)
    val sp = remaining.lastIndexOf(",")
    val titleDirty = splitted(1)
    val title =
      if (titleDirty.startsWith("\"")) titleDirty.drop(1) else titleDirty

    val year = Try(
      title
        .substring(title.lastIndexOf("("), title.lastIndexOf(")"))
        .drop(1)
        .toInt
    ).toOption
    val genres = remaining.substring(sp + 1).split('|').toList
    Movie(id, title, year, genres)
  }

  def parseRating(line: String): Rating = {
    val splitted = line.split(",")
    val userId = splitted(0).toLong
    val movieId = splitted(1).toInt
    val rating = splitted(2).toDouble
    val timestamp = splitted(3).toLong
    Rating(userId, movieId, rating, timestamp)
  }

  def parseTag(line: String) = {
    val splitted = line.split(",")
    val userId = splitted(0).toInt
    val movieId = splitted(1).toInt
    val tag = splitted(2)
    val timestamp = splitted(3).toLong
    Tag(userId, movieId, tag, timestamp)
  }
}
