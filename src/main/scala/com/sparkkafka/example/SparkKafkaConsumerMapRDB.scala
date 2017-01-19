package com.sparkkafka.example

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.v09.KafkaUtils


object SparkKafkaConsumerMapRDB {

  // Define variables (MapR-DB table, column family and column name)

  final val tableName = "/user/user01/smscall"
  final val cfDataBytes = Bytes.toBytes("data")
  final val colsquareIdBytes = Bytes.toBytes("squareId")
  final val coltimeIntervalBytes = Bytes.toBytes("timeInterval")
  final val colcountryCodeBytes = Bytes.toBytes("countryCode")
  final val colsmsInActivityBytes = Bytes.toBytes("smsInActivity")
  final val colsmsOutActivityBytes = Bytes.toBytes("smsOutActivity")
  final val colcallInActivityBytes = Bytes.toBytes("callInActivity")
  final val colcallOutActivityBytes = Bytes.toBytes("callOutActivity")
  final val colinternetTrafficActivityBytes = Bytes.toBytes("internetTrafficActivity")

  // Metadata

  case class CallDataRecord(squareId: Int, timeInterval: Long, countryCode: Int,
                            smsInActivity: Float, smsOutActivity: Float, callInActivity: Float,
                            callOutActivity: Float, internetTrafficActivity: Float)

  // Parsing the data

  object CallDataRecord extends Serializable {

    def parseCallDataRecord(str: String): CallDataRecord = {
      val c = str.split("\\t", -1).map(str => if (str.isEmpty()) "0" else str)
      CallDataRecord(c(0).toInt, c(1).toLong, c(2).toInt, c(3).toFloat, c(4).toFloat, c(5).toFloat, c(6).toFloat, c(7).toFloat)
    }

    // Create the put object

    def putCallDataRecord(callDataRecord: CallDataRecord): (ImmutableBytesWritable, Put) = {

      val rowkey = callDataRecord.squareId + "_" + callDataRecord.timeInterval + "_" + callDataRecord.countryCode
      val put = new Put(Bytes.toBytes(rowkey))

      put.addColumn(cfDataBytes, colsquareIdBytes, Bytes.toBytes(callDataRecord.squareId))
      put.addColumn(cfDataBytes, coltimeIntervalBytes, Bytes.toBytes(callDataRecord.timeInterval))
      put.addColumn(cfDataBytes, colcountryCodeBytes, Bytes.toBytes(callDataRecord.countryCode))
      put.addColumn(cfDataBytes, colsmsInActivityBytes, Bytes.toBytes(callDataRecord.smsInActivity))
      put.addColumn(cfDataBytes, colsmsOutActivityBytes, Bytes.toBytes(callDataRecord.smsOutActivity))
      put.addColumn(cfDataBytes, colcallInActivityBytes, Bytes.toBytes(callDataRecord.callInActivity))
      put.addColumn(cfDataBytes, colcallOutActivityBytes, Bytes.toBytes(callDataRecord.callOutActivity))
      put.addColumn(cfDataBytes, colinternetTrafficActivityBytes, Bytes.toBytes(callDataRecord.internetTrafficActivity))

      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)

    }
  }

  def main(args: Array[String]) = {

    // Define variables for MapR Stream

    val groupId = "smscall"
    val offsetReset = "earliest"
    val pollTimeout = "1000"
    val topicc = "/user/user01/stream:cdrp"
    val brokers = "localhost:9999"

    // Define Hbase connection

    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass) // ???
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // Define Spark context

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumerMapRDB.getClass.getName)

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = topicc.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val messagesDStream: InputDStream[(String, String)] = {
      KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)
    }

    val valuesDStream: DStream[String] = messagesDStream.map(_._2)

    valuesDStream.foreachRDD { rdd =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {

        // Parse RDD
        val parsedRDD = rdd.map(CallDataRecord.parseCallDataRecord)

        // Transform RDD into a put object and save in Hbase table
        parsedRDD.map(CallDataRecord.putCallDataRecord).saveAsHadoopDataset(jobConfig)

      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }


}

