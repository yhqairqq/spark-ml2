package com.unionpay.sparkmongo.app

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
  * Created by yhqairqq@163.com on 16/10/18.
  */
object demo12 {
  val db = "crawl_hz"
  val collection = "MainShop2"
  val collection_out = "MainShop3"
//  val uri = "mongodb://127.0.0.1:33332/"
      val uri = "mongodb://10.15.159.169:30000/"
  val connAliveTime = "15000000000"


  def save2mongo(uri: String, db: String, collection: String, df: DataFrame) {
    val conf = df.sqlContext.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms", connAliveTime)
      .set("spark.mongodb.output.uri", uri)
      .set("spark.mongodb.output.database", db)
      .set("spark.mongodb.output.collection", collection)

    val writeConfig = WriteConfig(conf)

    /**
      * 其它集中模式
      * SaveMode.ErrorIfExists (default)	"error" (default)
      * SaveMode.Append	"append"
      * SaveMode.Overwrite	"overwrite"
      * SaveMode.Ignore	"ignore"
      */

    df.write.mode(SaveMode.Overwrite).mongo(writeConfig)
  }

  def update2mongo(uri: String, db: String, collection: String, df: DataFrame) {
    val conf = df.sqlContext.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms", connAliveTime)
      .set("spark.mongodb.output.uri", uri)
      .set("spark.mongodb.output.database", db)
      .set("spark.mongodb.output.collection", collection)

    val writeConfig = WriteConfig(conf)

    /**
      * 其它集中模式
      * SaveMode.ErrorIfExists (default)	"error" (default)
      * SaveMode.Append	"append"
      * SaveMode.Overwrite	"overwrite"
      * SaveMode.Ignore	"ignore"
      */

    df.write.mode(SaveMode.Overwrite).mongo(writeConfig)
  }


  def mongoDF(ss: SparkSession, uri: String, db: String, collection: String): DataFrame = {
    val config = ss.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms", connAliveTime)
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.input.database", db)
      .set("spark.mongodb.input.collection", collection)
      //todo https://docs.mongodb.com/spark-connector/configuration/
      .set("spark.mongodb.input.readPreference.name", "primaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
      .set("spark.mongodb.input.partitionerOptions.partitionSizeMB", "1024")
    val readConfig = ReadConfig.create(config)
    ss.read.mongo(readConfig)

  }

  def main(args: Array[String]) {


    val MASTER_URL = "spark://127.0.0.1:7077"


    val sparkConf = new SparkConf()
      .setMaster(MASTER_URL)
      .set("spark.executor.memory","2g")
      .set("spark.executor.cores","4")
      .set("spark.executor.cores","4")
      .setAppName("kmeans")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()



    (1.0 to Int.MaxValue by 1).par.map(k=>(k,math.log(k))).toList.foreach(println)

    spark.sparkContext.stop()

  }

}
