import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BetsStreamProcessingTask extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("bets-streaming-task")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext

  sc.setLogLevel("WARN")

  import spark.implicits._

  val betSchema = new StructType()
    .add("event_id", LongType)
    .add("event_time", TimestampType)
    .add("player_id", StringType)
    .add("bet", DoubleType)
    .add("game_name", StringType)
    .add("country", StringType)
    .add("win", DoubleType)
    .add("online_time_secs", LongType)
    .add("currency_code", StringType)

  val betsDF =
    spark
      .readStream
      .option("multiline", true)
      .schema(betSchema)
      .json("resources/bets/*")


  val filteredBetsUSD = betsDF
    .filter($"country" === "US" && !$"game_name".endsWith("_demo"))
    .withColumn("bet", when($"currency_code" === "EUR", $"bet" / 1.1).otherwise($"bet"))
    .withColumn("win", when($"currency_code" === "EUR", $"win" / 1.1).otherwise($"win"))
    .withWatermark("event_time", "20 seconds")

  val windowedStatsPerGame = filteredBetsUSD
    .groupBy(window($"event_time", "10 seconds"), $"game_name")
    .agg(
      max("bet") as "max_bet",
      min("bet") as "min_bet",
      avg("bet") as "avg_bet",
      sum("bet") as "total_bets",
      max("win") as "max_win",
      min("win") as "min_win",
      avg("win") as "avg_win",
      sum("win") as "total_wins",
      sum($"bet" - $"win") as "profit"
    )
    .withColumn("window_end", $"window.end")
    .drop("window")


  val windowedProfit = filteredBetsUSD
    .groupBy(window($"event_time", "10 seconds"))
    .agg(sum($"bet" - $"win") as "profit")
    .withColumn("window_end", $"window.end")
    .drop("window")


  def winBetsForEachBatchSplitter(batch: DataFrame, id: Long): Unit = {
    batch.persist()
    batch.show(10, truncate = false)

    batch
      .select("window_end", "game_name", "max_bet", "min_bet", "avg_bet")
      .write
      .format("json")
      .mode(SaveMode.Append)
      .save("resources/output/stats/bet")

    batch
      .select("window_end", "game_name", "max_win", "min_win", "avg_win")
      .write
      .format("json")
      .mode(SaveMode.Append)
      .save("resources/output/stats/win/")

    batch
      .select("window_end", "game_name", "profit")
      .write
      .format("json")
      .mode(SaveMode.Append)
      .save("resources/output/stats/profit/")

    batch.unpersist()
  }

  windowedStatsPerGame
    .writeStream
    .option("checkPointLocation", "resources/checkpoints/bets")
    .foreachBatch(winBetsForEachBatchSplitter _)
    .start()

  windowedProfit
    .writeStream
    .outputMode(OutputMode.Append())
    .format("json")
    .option("checkpointLocation", "resources/checkpoints/overall_profit")
    .option("path", "resources/output/overall_profit")
    .start()

  spark
    .streams
    .awaitAnyTermination()
}
