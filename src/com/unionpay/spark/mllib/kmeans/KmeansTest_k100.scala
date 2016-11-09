package com.unionpay.spark.mllib.kmeans

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

/**
  * Created by yhqairqq@163.com on 2016/11/7.
  */
object KmeansTest_k100 {

  def distance(point1: Vector, point2: Vector) = {

    math.sqrt(point1.toArray.zip(point2.toArray).map(x => x._1 - x._2).map(d => d * d).sum)
  }

  def distToCentroid(point: Vector, model: KMeansModel) = {

    val cluster = model.predict(point)

    val centroid = model.clusterCenters(cluster)

    distance(centroid, point)
  }

  def clusteringScore(data:RDD[Vector],k:Int)={
    val kmeans = new KMeans()

    kmeans.setK(k)
    val model = kmeans.run(data)

    data.map(datum=>distToCentroid(datum,model)).mean()
  }


  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
//                  .setMaster("local")
      .setAppName("kmeans")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

//   val  rawData =  spark.read.option("inferSchema", true).
//      option("header", false).textFile("/Users/YHQ/baiduyun/kddcup.data")

    val rawData = spark.sparkContext.textFile("kddcup.data")
//    val rawData = spark.sparkContext.textFile("/Users/YHQ/baiduyun/kddcup.data")

//    rawData.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse
//      .foreach(println)


    val labelsAndData = rawData.map(line => {
      val buffer = line.split(",").toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)

      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)

      (label, vector)
    })

    val data = labelsAndData.values.cache()

    val kmeans = new KMeans()
    kmeans.setK(100)
    val model = kmeans.run(data)

    val sample = data.map(datum=>{
      model.predict(datum)+","+datum.toArray.mkString(",")
    }).sample(false,0.05)

    sample.saveAsTextFile("/user/ds/smaple2")

    spark.sparkContext.stop()
  }

}
