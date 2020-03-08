package com.lsd.etl.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 搜狗用户查询日志(SogouQ)数据 WordCount 案例
 * <p>
 * Created by lsd
 * 2020-03-01 12:43
 */
public class SparkETLDemo {

    public static void main(String[] args) {
        // 环境变量
        System.setProperty("HADOOP_USER_NAME", "root");
        // Setting Master for running it
        SparkConf sparkConf = new SparkConf()
                .setAppName("hot word")
                .setMaster("spark://spark-master:7077") //提交到Spark执行
                .setJars(new String[]{"/spark-demo1-1.0-SNAPSHOT.jar"});  //设置分发到集群的jar，非local模式必须配置否则ClassCastException

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // 配置hadoop
        Configuration hadoopConf = sparkContext.hadoopConfiguration();
        // 必须有这个设置，否则No FileSystem for scheme: hdfs
        hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());


        // 读取一个数据集
        JavaRDD<String> linesRdd = sparkContext.textFile("hdfs://namenode:8020/SogouQ.sample.txt");
        /**
         * map操作
         * 一行数据 -> Tuple2<String,Integer>
         */
        JavaPairRDD<String, Integer> wordPairRDD = linesRdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                // 获取搜索词
                final String word = s.split("\t")[2];
                return new Tuple2<>(word, 1);
            }
        });

        /**
         * reduce操作，分组统计每个词的数量
         */
        JavaPairRDD<String, Integer> wordCountRDD = wordPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * 为了按词数排序，需要互换键值对位置再调用sortByKey()
         *
         * ("word1",2) -> (2,"word1")
         */
        JavaPairRDD<Integer, String> countWordRDD = wordCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();   //互换键值对位置
            }
        });

        // 按词数倒序排序，求top10
        JavaPairRDD<Integer, String> sortedCountWordRDD = countWordRDD.sortByKey(false);
        // 键值对位置换回来
        JavaPairRDD<String, Integer> resultRDD = sortedCountWordRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();   //互换键值对位置
            }
        });
        resultRDD.take(10)
                .forEach(tuple -> System.out.println(tuple._1() + "===" + tuple._2()));
    }
}
