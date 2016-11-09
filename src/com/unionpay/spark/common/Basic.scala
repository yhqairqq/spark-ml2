package com.unionpay.spark.common

import java.sql.{ResultSet, DriverManager}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by yhqairqq@163.com on 16/11/1.
  */
object Basic {
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.getConnection("jdbc:mysql://localhost/zoo?user=root");
  }

  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }

  def main(args: Array[String]) {
    val master = args(0)
    val sc = new SparkContext(master, "LoadSimpleJdbc", System.getenv("SPARK_HOME"))
    val data = new JdbcRDD(sc,
      createConnection, "SELECT * FROM panda WHERE ? <= id AND ID <= ?",
      lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

}
