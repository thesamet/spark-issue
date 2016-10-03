import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.streaming._

object ProtoDemo {
  def createContext(dirName: String) = {
    val conf = new SparkConf().setAppName("mything").setMaster("local[4]")
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    /*
    conf.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
    conf.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
    */

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(dirName)
    val lines = ssc.socketTextStream("127.0.0.1", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    val runningCounts = wordCounts.updateStateByKey[Int] {
      (values: Seq[Int], oldValue: Option[Int]) =>
        val s = values.sum
        Some(oldValue.fold(s)(_ + s))
      }

  // Print the first ten elements of each RDD generated in this DStream to the console
    runningCounts.print()
    ssc
  }

  def main(args: Array[String]) = {
    val hadoopConf = new Configuration()
    val dirName = "/tmp/chkp"
    val ssc = StreamingContext.getOrCreate(dirName, () => createContext(dirName), hadoopConf)
    ssc.start()
    ssc.awaitTermination()
  }

}
