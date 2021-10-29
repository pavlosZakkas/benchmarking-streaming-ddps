package aggregator

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Timestamp
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType, LongType}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */

object PurchaseAggregatorDataFrame {
  case class GemPurchase(key: String, time: Timestamp, price: Double)

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: PurchaseAggregatorDataFrame <hostname> <port>")
      System.exit(1)
    }
    val spark = SparkSession
      .builder
      .appName("Purchase")
      //      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val host = args(0)
    val port = args(1).toInt

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made progress: " + queryProgress.progress)
      }
    })

    val df = (spark
      .readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
      ).as[String]

    val gems = df.map(value => {
      val cols = value.split(",")
      GemPurchase(cols(0), new Timestamp(cols(1).toLong), cols(2).toDouble)
    })

    val windowed = gems.groupBy(
      window($"time", "8 seconds", "4 seconds"), $"key"
    ).sum("price")

    val res = windowed
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Complete())

    res.start()
      .awaitTermination()
  }
}
