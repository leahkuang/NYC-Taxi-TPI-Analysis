package unibo.bigdata.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OptimizedApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Tipping - Optimized")
      .getOrCreate()

    import spark.implicits._

    // 🌟 优化 1：根据 AWS 集群核心数精细化调整 shuffle 分区数，避免默认的 200 分区产生过多小文件和网络开销
    spark.conf.set("spark.sql.shuffle.partitions", "32")

    val tripsPath = args(0) // 🌟 建议在 AWS 上运行时传入 Parquet 格式的路径
    val zonesPath = args(1)
    val outputPath = args(2)

    // 🌟 优化 2：采用 Parquet 格式读取（具有极佳的列式裁剪和高压缩比）
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

    // 🌟 优化 3：由于 zonesDF（几百 KB）极小，显式调用 broadcast 强制启用广播连接
    // 这样可以直接消灭掉第一个 Join 的 Shuffle（无 Exchange 节点）
    val joinedDF = cleanedTripsDF.join(
      broadcast(zonesDF),
      cleanedTripsDF("PULocationID") === zonesDF("LocationID")
    )

    // 第一次 GroupBy 聚合
    val baselineDF = joinedDF
      .groupBy("Borough", "distance_bucket", "hour_of_day")
      .agg(avg("tip_percentage").as("base_tip_pct"))

    // 🌟 优化 4：由于 baselineDF 数据量也很小（Borough * 3种距离 * 24小时 约 500 行），
    // 在第二次关联时同样使用广播连接！这直接将第二个大 Join 的 Shuffle 也消灭了！
    val indexedTripsDF = joinedDF.join(
      broadcast(baselineDF),
      Seq("Borough", "distance_bucket", "hour_of_day")
    )

    val tpiDF = indexedTripsDF
      .withColumn("tpi", $"tip_percentage" / $"base_tip_pct")

    // 最终聚合
    val finalReport = tpiDF
      .groupBy("Borough", "Zone")
      .agg(
        avg("tpi").as("average_tpi"),
        count("*").as("total_trips")
      )
      .filter($"total_trips" > 100)
      .sort($"average_tpi".desc)

    // 写入 Parquet 结果文件，速度更快
    finalReport.write
      .mode("overwrite")
      .parquet(outputPath)

    spark.stop()


  }
}
