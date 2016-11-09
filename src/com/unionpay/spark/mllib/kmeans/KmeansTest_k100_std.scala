package com.unionpay.spark.mllib.kmeans

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession

/**
  * Created by yhqairqq@163.com on 2016/11/7.
  */
object KmeansTest_k100_std {

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


  def entropy(counts:Iterable[Int])={
    val values = counts.filter(_>0)
    val n:Double = values.sum

    values.map{  v=>
      val p = v/n
      -p*math.log(p)
    }.sum
  }


  def clusteringScore2(normalizedLabelsAndData:RDD[(String,Vector)],k:Int)={

    val kmeans = new KMeans()
    val model = kmeans.run(normalizedLabelsAndData.values)

    val labelsAndClusters =
      normalizedLabelsAndData.mapValues(model.predict) //对每个数据预测簇类别

    val clustersAndLabels = labelsAndClusters.map(_.swap)  //对换键和值

    val labelInCluster = clustersAndLabels.groupByKey().values  //按簇提取标号集合

    val labelCounts = labelInCluster.map(_.groupBy(l=>l).map(_._2.size)) //计算集合中各簇标号出现对次数

    val n = normalizedLabelsAndData.count()  //

    labelCounts.map(m=>m.sum*entropy(m)).sum/n //根据簇大小计算熵的加权平均
  }



  def main(args: Array[String]) {


    val MASTER_URL = "spark://127.0.0.1:7077"

    val MASTER_LOCAL="local"

    val sparkConf = new SparkConf()
//                  .setMaster(MASTER_LOCAL)
      .setAppName("kmeans")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

//   val  rawData =  spark.read.option("inferSchema", true).
//      option("header", false).textFile("/Users/YHQ/baiduyun/kddcup.data")

    val rawData = spark.sparkContext.textFile("kddcup.data")
//    val rawData = spark.sparkContext.textFile("/Users/YHQ/baiduyun/kddcup.data")

    rawData.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse
      .foreach(println)


    val labelsAndData = rawData.map(line => {
      val buffer = line.split(",").toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)

      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)

      (label, vector)
    })

    val data = labelsAndData.values.cache()


    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length

    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a,b)=>a.zip(b).map(t=>t._1+t._2)
    )
      val sumSquares = dataAsArray.fold(
        new Array[Double](numCols)
      )(
        (a,b)=>a.zip(b).map(t=>t._1+t._2*t._2)
      )
    val stdevs = sumSquares.zip(sums).map{
      case(sumSq,sum)=>math.sqrt(n*sumSq-sum*sum)/n
    }
    val means = sums.map(_/n)

    def normalize(datum:Vector)={
      val normalizedArray = (datum.toArray,means,stdevs).zipped
        .map(
          (value,mean,stdev)=>
            if(stdev<=0)(value-mean)else(value-mean)/stdev
        )
      Vectors.dense(normalizedArray)
    }


    val normalizedData = data.map(normalize).cache()

    (60 to 100 by 10).par.map(k=>(k,clusteringScore(normalizedData,k))).toList.foreach(println)

    spark.sparkContext.stop()
  }

}
