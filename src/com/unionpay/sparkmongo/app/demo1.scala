package com.unionpay.sparkmongo.app

import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhqairqq@163.com on 16/10/18.
  */
object demo1 {
  val db = "crawl_hz"
  val collection = "MainShop2"
  val uri = "mongodb://127.0.0.1:33332/"
//  val uri = "mongodb://10.15.159.169:30000/"
  val connAliveTime = "15000"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("scalaSimple1")
    //    .setMaster("local")
    val sc = new SparkContext(sparkConf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val config = sqlContext.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms", "15000")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.input.database", db)
      .set("spark.mongodb.input.collection", collection)
      //todo https://docs.mongodb.com/spark-connector/configuration/
      .set("spark.mongodb.input.readPreference.name", "primaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
      .set("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1024")

    val readConfig = ReadConfig(config)

    val df = sqlContext.read.format("com.mongodb.spark.sql").mongo(readConfig)

    df.printSchema()

    df.filter(df("age")>=12 && df("age")<13).show()


//    case class Person(_id:String, name: String, age: Int)
//
//    val explicitDF = MongoSpark.load(sqlContext)
//
//    explicitDF.printSchema()
    //    sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://10.15.159.169:30000/crawl_hz.MainShop2"))) // Uses the ReadConfig

    //    sqlContext.read.mongo()


  }

}
