package com.unionpay.sparkmongo.app

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
/**
  * Created by yhqairqq@163.com on 16/10/18.
  */
object demo5 {
  val db = "crawl_hz"
  val collection = "MainShop2"
  val collection_out = "MainShop3"
  val uri = "mongodb://127.0.0.1:33332/"
//    val uri = "mongodb://10.15.159.169:30000/"
  val connAliveTime = "15000"


  def save2mongo(uri:String,db:String,collection:String,df:DataFrame){
    val conf =   df.sqlContext.sparkContext.getConf
        .set("spark.mongodb.keep_alive_ms", connAliveTime)
        .set("spark.mongodb.output.uri", uri)
        .set("spark.mongodb.output.database", db)
        .set("spark.mongodb.output.collection", collection)

    val writeConfig = WriteConfig(conf)
    df.write.mode(SaveMode.Overwrite).mongo(writeConfig)
  }


  def mongoDF(ss: SparkSession,uri:String,db:String,collection:String):DataFrame={
    val config = ss.sparkContext.getConf
      .set("spark.mongodb.keep_alive_ms",connAliveTime)
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

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("scalaSimple2")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    val df = mongoDF(spark,uri,db,collection)
    df.printSchema()
    df.createOrReplaceTempView("MainShop")
    val df_filter = spark.sql("SELECT * FROM MainShop WHERE age >=12")
    df_filter.show()




//
//    save2mongo(uri,db,collection_out,centenarians)



    //    case class Person(_id:String, name: String, age: Int)
    //
    //    val explicitDF = MongoSpark.load(sqlContext)
    //
    //    explicitDF.printSchema()
    //    sqlContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://10.15.159.169:30000/crawl_hz.MainShop2"))) // Uses the ReadConfig

    //    sqlContext.read.mongo()


  }

}
