
package aggregator

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStopped}

object PurchaseAggregatorRDD {
  case class GemPurchase(key: String, time: Long, price: Double)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: PurchaseAggregatorRDD <hostname> <port> <file-path>")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .appName("PurchaseAggregatorRDD")
      .getOrCreate()
    val sparkConf = spark.sparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        val submissionTime = stageCompleted.stageInfo.submissionTime.get
        val completionTime = stageCompleted.stageInfo.completionTime.get
        val resultSize = stageCompleted.stageInfo.taskMetrics.resultSize
        val recordsRead = stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead
        println(s"${recordsRead},${resultSize},${System.currentTimeMillis()},${completionTime - submissionTime}")
      }
    })

    val input = ssc.socketTextStream(args(0), args(1).toInt)
    val purchases = input.map(x => {
      val split = x.split(",")
      (split(0), GemPurchase(split(0), split(1).toLong, split(2).toDouble))
    })

    val windowed = purchases.window(Seconds(8), Seconds(4))
      .reduceByKey((a: GemPurchase, b: GemPurchase) => GemPurchase(b.key, a.time.max(b.time), b.price + a.price))
      .mapValues((gem: GemPurchase) => {
        val curTime = System.currentTimeMillis()
        (curTime, curTime - gem.time)
      }).saveAsTextFiles(args(2), "csv")

    ssc.start()
    ssc.awaitTermination()
  }
}
