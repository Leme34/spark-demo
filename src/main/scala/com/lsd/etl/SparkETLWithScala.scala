package com.lsd.etl

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 搜狗用户查询日志(SogouQ)数据 统计搜狗排名与用户排名序号一致的记录
 *
 * Created by lsd
 * 2020-03-01 18:02
 */
object SparkETLWithScala {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
      .setMaster("spark://spark-master:7077") //本地1个线程运行
      .setAppName("hot word scala")
      .setJars(List("/spark-demo1-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(config)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    hadoopConf.set("fs.file.impl", classOf[LocalFileSystem].getName)

    val lines = sc.textFile("hdfs://namenode:8020/SogouQ.sample.txt")
    val total = lines.count()
    val hitCount = lines.map(x => x.split("\t")(3)) //获取搜狗排名与用户排名信息块
      .map(word => word.split(" ")(0).equals(word.split(" ")(1))) //切出搜狗排名与用户排名并判断是否相等，转换为布尔值
      .filter(x => x == true)
      .count()
    println("hit rate:" + (hitCount.toFloat / total.toFloat))
  }
}
