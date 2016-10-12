import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import com.datastax.spark.connector._ 
 
 
object StructStream {
  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.31.0.174")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession.builder.appName("StructuredAverage").getOrCreate()
    import spark.implicits._
 
    /* Create the schema for messages in json format, include a timestamp to perform event time windowing */
    val userSchema = new StructType().add("id", "string").add("hr", "integer").add("time","timestamp")
    
    /* Read files written in a directory as a stream of data. */
    val jsonDF = spark.readStream.schema(userSchema).json("hdfs://ec2-52-45-70-95.compute-1.amazonaws.com:9000/sensor/")

    /* Perform averaging over a specified event time window in Structured Streaming, simply using groupby method */
    val line_avg = jsonDF.groupBy(window($"time","2 minutes","1 minutes"), $"id")
                    .agg(avg("hr"), max("hr"), min("hr"))
                    .orderBy("window")


    /* Try to use for each sink to write data to cassandra, BUT this does not work well */
    import org.apache.spark.sql.ForeachWriter

    /* One need to specify the open connection, process, and close connection function explicitly in the writer */
    val writer = new ForeachWriter[org.apache.spark.sql.Row] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: org.apache.spark.sql.Row) = {
        /* Convert the result to strings */
        val toRemove = "[]".toSet
        val v_str = value.toString().filterNot(toRemove).split(",")
        /*Convert them to dataframe*/
        Seq(Stick(v_str(2),v_str(3).toInt,v_str(1),v_str(0))).toDF()
            .write.format("org.apache.spark.sql.cassandra")
            .options(Map("table"->"sstest","keyspace"->"playground"))
            .mode(SaveMode.Append).save()
        }
      override def close(errorOrNull: Throwable) = ()
    }
 
    val query = line_avg.writeStream.outputMode("complete").foreach(writer).start()

    /* For this version, the data is written into memory sink*/
    val query = line_avg.writeStream.outputMode("complete").format("memory").queryName("result_table").start()
    
    query.awaitTermination()
 
  }
 
}
 
case class Stick(id: String, avg:Int, endt: String, sst: String)
