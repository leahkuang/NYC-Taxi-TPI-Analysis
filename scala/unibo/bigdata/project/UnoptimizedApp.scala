package unibo.bigdata.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UnoptimizedApp {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("NYC Taxi Tipping - Unoptimized")
      .getOrCreate()

    import spark.implicits._

    // 🌟 禁用广播 Join，强行在两个 Join 阶段都触发 Shuffle
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    // 读取输入路径（从命令行参数传入，方便本地和 AWS 切换）
    val tripsPath = args(0) // 5GB 的大 CSV 文件路径
    val zonesPath = args(1) // 几百 KB 的 Zone Lookup CSV 路径
    val outputPath = args(2)

    // 加载原始大表数据 (CSV 格式)
    val tripsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(tripsPath)

    // 加载 Zone 查找表
    val zonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zonesPath)

    // 数据清洗与预处理：过滤异常值并对距离分桶
    val cleanedTripsDF = tripsDF
      .filter($"trip_distance" > 0 && $"fare_amount" > 0 && $"tip_amount" >= 0)
      .withColumn("tip_percentage", ($"tip_amount" / $"fare_amount") * 100)
      .withColumn("hour_of_day", hour($"tpep_pickup_datetime"))
      .withColumn("distance_bucket",
        when($"trip_distance" <= 2.0, "Short")
          .when($"trip_distance" > 2.0 && $"trip_distance" <= 8.0, "Medium")
          .otherwise("Long")
      )

    // 【Shuffle 1】: 执行普通的 Shuffle Join (SortMergeJoin) 将大表与 Zone 表关联
    val joinedDF = cleanedTripsDF.join(
      zonesDF,
      cleanedTripsDF("PULocationID") === zonesDF("LocationID")
    )

    // 【Shuffle 2】: 第一次 GroupBy 聚合，计算每个 Borough 结合时间段和距离的小费基准线
    val baselineDF = joinedDF
      .groupBy("Borough", "distance_bucket", "hour_of_day")
      .agg(avg("tip_percentage").as("base_tip_pct"))

    // 【Shuffle 3】: 将清洗后的明细数据与刚才算出的基准线进行第二次 Shuffle Join
    val indexedTripsDF = joinedDF.join(
      baselineDF,
      Seq("Borough", "distance_bucket", "hour_of_day")
    )

    // 计算小费指数 TPI
    val tpiDF = indexedTripsDF
      .withColumn("tpi", $"tip_percentage" / $"base_tip_pct")

    // 【Shuffle 4 / Final Aggregation】: 最终聚合，计算每个具体 Zone 的平均 TPI 指数并排序
    val finalReport = tpiDF
      .groupBy("Borough", "Zone")
      .agg(
        avg("tpi").as("average_tpi"),
        count("*").as("total_trips")
      )
      .filter($"total_trips" > 100) // 过滤掉样本极少的地方
      .sort($"average_tpi".desc)

    // 将结果写入到目标路径
    finalReport.coalesce(1).write
      .option("header", "true")
      .csv(outputPath)

    spark.stop()


  }
}
