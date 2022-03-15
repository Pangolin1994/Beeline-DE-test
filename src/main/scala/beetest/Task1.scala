package beetest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger, Level}


object Task1 {
  def job(): Unit = {
    // turn off redundant console info
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val config = new SparkConf().setMaster("local[4]")
      .setAppName("Beeline test jobs")
    val session = SparkSession.builder
      .config(config)
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    import session.implicits._

    val booksDF = session.read
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv("resources/books.csv")

    booksDF.printSchema()

    println($"Кол-во записей ${booksDF.count()}\n")

    val highRatedBooksDF = booksDF.filter($"average_rating" > 4.5)
    highRatedBooksDF.show(10, truncate=false)

    val averageRating = booksDF.select(avg($"average_rating"))
    averageRating.show()

    val less5RatingBooksDF = booksDF.where(
      $"average_rating".isNotNull && $"average_rating".between(0, 5)
    )

    val (countIntervals, interval, threshold) = (5, 1, 4)
    val range = less5RatingBooksDF.withColumn("rating", $"average_rating".cast(IntegerType))
      .withColumn(
        "range", when($"rating" < countIntervals, $"rating").otherwise(threshold)
      )
      .withColumn(
        "range", concat($"range", lit(" - "), $"range" + interval)
      )
      .groupBy($"range")
      .count()
      .orderBy($"range")
    range.show(false)
  }
}
