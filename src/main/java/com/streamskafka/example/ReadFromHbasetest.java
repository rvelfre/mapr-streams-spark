package com.streamskafka.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import scala.Tuple2;



public class ReadFromHbasetest {

    public static void main(String[] args)  {

        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        Configuration config = null;
        try {
            config = HBaseConfiguration.create();
            config.set(TableInputFormat.INPUT_TABLE, "/user/userinfo");
        }
        catch (Exception ce){
            ce.printStackTrace();
        }

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                sc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        //Long rows = hBaseRDD.count();
        //System.out.println(rows);

        JavaPairRDD rowPairRDD = hBaseRDD.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String>() {
                    @Override
                    public Tuple2 call(
                            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

                        Result r = entry._2;
                        String keyRow = Bytes.toString(r.getRow());
                        String value = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));

                        return new Tuple2(keyRow, value);
                    }
                });

        DataFrame myDF =   sqlContext.createDataFrame(rowPairRDD.values(), Tuple2.class);

        myDF.write().parquet("/tmp/testparquet");

    }

}
