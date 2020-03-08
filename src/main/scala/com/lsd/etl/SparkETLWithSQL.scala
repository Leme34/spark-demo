package com.lsd.etl

import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 搜狗用户查询日志(SogouQ)数据 统计搜狗排名与用户排名序号一致的记录
 *
 * Created by lsd
 * 2020-03-01 18:02
 */
object SparkETLWithSQL {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
      .setAppName("hot word scala")
      .setJars(List("/spark-demo1-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(config)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    hadoopConf.set("fs.file.impl", classOf[LocalFileSystem].getName)

    val lines = sc.textFile("hdfs://namenode:8020/SogouQ.sample.txt")
    // 每一行数据(SQL Row)的定义
    val structType = StructType(
      // int型 not null
      StructField("rank", IntegerType, false) :: //add to List
        StructField("click", IntegerType, false) :: Nil
    )
    // 行数据(SQL Row)
    val row = lines.map(line => {
      val arr = line.split("\t")
      val rank_click = arr(3).split(" ")
      Row(rank_click(0).toInt, rank_click(1).toInt)
    })
    // 使用Spark Session创建表
    val ss = SparkSession.builder().getOrCreate()
    val df = ss.createDataFrame(row, structType) // 使用行创建表结构
    df.createOrReplaceTempView("tb") //  创建表
    // 查询SQL
    val re = df.sqlContext.sql("select count(if(t.rank=t.click,1,null)) as hit, count(1) as total from tb as t ")
    re.show()
    // 结果集迭代器
    val next = re.toLocalIterator().next()
    // 因为count只有一条数据，取出对应列即可
    val hit = next.getAs[Long]("hit")
    val total = next.getAs[Long]("total")
    println(hit.toFloat / total.toFloat)
  }
}
