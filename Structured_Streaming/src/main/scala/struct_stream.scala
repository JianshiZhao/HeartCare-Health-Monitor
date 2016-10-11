import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector._
 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql._
import org.apache.spark._
import com.datastax.spark.connector.types.CassandraOption
 
 
 
object StructStream {
  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.31.0.174")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession.builder.appName("StructuredAverage").getOrCreate()
    import spark.implicits._
 
    val userSchema = new StructType().add("id", "string").add("hr", "integer").add("time","timestamp")

    val jsonDF = spark.readStream.schema(userSchema).json("hdfs://ec2-52-45-70-95.compute-1.amazonaws.com:9000/test3/")

    val line_avg = jsonDF.groupBy(window($"time","2 minutes","1 minutes"), $"id").agg(avg("hr"), max("hr"), min("hr")).orderBy("window")


    /* Try to use for each sink to write data to cassandra, BUT this does not work well */
    import org.apache.spark.sql.ForeachWriter
 
    val writer = new ForeachWriter[org.apache.spark.sql.Row] {
      override def open(partitionId: Long, version: Long) = true
      override def process(value: org.apache.spark.sql.Row) = {
        val toRemove = "[]".toSet
        val v_str = value.toString().filterNot(toRemove).split(",")
        Seq(Stick(v_str(2),v_str(3).toInt,v_str(1),v_str(0))).toDF().write.format("org.apache.spark.sql.cassandra").options(Map("table"->"sstest","keyspace"->"playground")).mode(SaveMode.Append).save()
        println(v_str(0),v_str(1),v_str(2),v_str(3))}
      override def close(errorOrNull: Throwable) = ()
    }
 
    val query = line_avg.writeStream.outputMode("complete").foreach(writer).start()

    /* For this version, the data is written into memory sink*/
    val query = line_avg.writeStream.outputMode("complete").format("memory").queryName("result_table").start()
    
    query.awaitTermination()
 
  }
 
}
 
case class Stick(id: String, count:Int, endt: String, sst: String)
