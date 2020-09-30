package com.atquiqu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Classname WordCount
 * @Description TODO
 * @Date 2020/9/30 10:12
 * @Created by 林立
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    //使用开发工具完成spark workCount开发
    /** 创建spark config对象
     *1.设定spark计算框架运行（部署）环境
     *2.设置应用名
     * */
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //spark conunt 创建
    val sc = new SparkContext(config)
    /*
    * 1.读取文件为行数据
    * 2.扁平化处理，按照空格将行数据处理成一个个单词
    * 3.为了方便统计，将数据结构转换成（"hello",1）
    * 4.对转化数据进行分组聚合
    * */
    val lines: RDD[String] = sc.textFile("in")
    val wodrs: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = wodrs.map( (_, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val result: Array[(String, Int)] = wordToSum.collect()
    
    //打印控制台
    result.foreach(println) 
    
  }

}
