package beetest

import java.nio.file.Paths
import java.io.{FileWriter, File}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import io.circe._
import io.circe.syntax._


object Task2 {
  case class Info(ratings: Seq[Int], ratingsCount: Seq[Long], title: String) {
    lazy val map: Map[Int, Long] = ratings.iterator.zip(ratingsCount).toMap
  }

  case class ExtendedInfo(
    ratings: Seq[Int], ratingsCount: Seq[Long], extendedRatingsCount: Seq[Long], title: String
  )

  def job(): Unit = {
    val config = new SparkConf().setMaster("local[*]")
      .setAppName("Beeline test jobs")
    val session = SparkSession.builder
      .config(config)
      .getOrCreate()
    import session.implicits._

    session.conf.set("spark.sql.shuffle.partitions", 40)
    val path = Paths.get("resources/data/ml-100k")
    // Items dataframe
    val genres = Array("unknown", "Action", "Adventure", "Animation", "Children's",
      "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror",
      "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"
    )
    val markFields = genres.map(g => StructField(g, ByteType, nullable=false))
    val itemSchema = StructType(
      Array(
        StructField("movie id", IntegerType, nullable=false),
        StructField("title", StringType, nullable=true),
        StructField("date", DateType, nullable=true),
        StructField("video date", ByteType, nullable=true),
        StructField("url", StringType, nullable=true)
      ) ++ markFields
    )
    val itemsDF = session.read
      .option("delimiter", "|")
      .schema(itemSchema)
      .csv(path.resolve("u.item").toString)
      .select("movie id", "title")

    // Films dataframe
    val dataSchema = StructType(
      Array(
        StructField("user id", IntegerType, nullable=false),
        StructField("item id", IntegerType, nullable=true),
        StructField("rating", IntegerType, nullable=true),
        StructField("timestamp", LongType, nullable=true)
      )
    )
    val dataDF = session.read
      .option("delimiter", "\t")
      .schema(dataSchema)
      .csv(path.resolve("u.data").toString)
      .select("item id", "rating")

    val joinedMoviesDF = dataDF.join(
      itemsDF, col("item id") === col("movie id"), "inner"
    ).select("title", "rating", "movie id")

    // 1
    val allFilmsRatings = joinedMoviesDF.groupBy(col("rating"))
      .agg("rating" -> "count")
      .orderBy(col("rating"))
      .agg(collect_list(col("count(rating)")))
      .head()

    // 2
    val filmFilteredDF = joinedMoviesDF.filter($"movie id" === 32)
      .select("title", "rating")
    // Get film title
    val filmTitle = filmFilteredDF.head().getString(0)
    val ratingByFilmCountDF = filmFilteredDF.groupBy(col("rating"))
      .agg(count("rating"))
      .agg(
        collect_list("rating").alias("ratings"),
        collect_list("count(rating)").alias("ratingsCount")
      )
      .withColumn("title", lit(filmTitle))
      .as[Info].map { info =>
        val result = (1 to 5).map(info.map.getOrElse(_, 0L))
        ExtendedInfo(info.ratings, info.ratingsCount, result, info.title)
      }

    val filmRatings = ratingByFilmCountDF.select(
      col("title"), col("extendedRatingsCount")
    ).head()

    val resultedJson = Json.obj(
      (filmRatings.getString(0), filmRatings.getSeq[Long](1).asJson),
      ("hist_all", allFilmsRatings.getSeq[Long](0).asJson)
    )

    val writer = new FileWriter(new File("resources/data/out.json"))
    writer.write(resultedJson.toString())
    writer.close()

    session.stop()
  }
}
