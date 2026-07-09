package unibo.bigdata.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OptimizedApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Tipping - Optimized")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", "32")

    val tripsPath = args(0) 
    val zonesPath = args(1)
    val outputPath = args(2)

    val tripsDF = spark.read.parquet(tripsPath)

    val zonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zonesPath)

    val cleanedTripsDF = tripsDF
      .filter($"trip_distance" > 0 && $"fare_amount" > 0 && $"tip_amount" >= 0)
      .withColumn("tip_percentage", ($"tip_amount" / $"fare_amount") * 100)
      .withColumn("hour_of_day", hour($"tpep_pickup_datetime"))
      .withColumn("distance_bucket",
        when($"trip_distance" <= 2.0, "Short")
          .when($"trip_distance" > 2.0 && $"trip_distance" <= 8.0, "Medium")
          .otherwise("Long")
      )

    val joinedDF = cleanedTripsDF.join(
      broadcast(zonesDF),
      cleanedTripsDF("PULocationID") === zonesDF("LocationID")
    )

    val baselineDF = joinedDF
      .groupBy("Borough", "distance_bucket", "hour_of_day")
      .agg(avg("tip_percentage").as("base_tip_pct"))

    val indexedTripsDF = joinedDF.join(
      broadcast(baselineDF),
      Seq("Borough", "distance_bucket", "hour_of_day")
    )

    val tpiDF = indexedTripsDF
      .withColumn("tpi", $"tip_percentage" / $"base_tip_pct")

    val finalReport = tpiDF
      .groupBy("Borough", "Zone")
      .agg(
        avg("tpi").as("average_tpi"),
        count("*").as("total_trips")
      )
      .filter($"total_trips" > 100)
      .sort($"average_tpi".desc)

    finalReport.write
      .mode("overwrite")
      .parquet(outputPath)

    spark.stop()


  }
}
