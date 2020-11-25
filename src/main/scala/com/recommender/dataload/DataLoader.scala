package com.recommender.dataload


import org.apache.spark.sql.SparkSession

import com.recommender.configs.UserSparkConf
import com.recommender.configs.UserMongoDBConf

object DataLoader{
    val reviewsDataPath="/usr/local/recommend/Magazine_Subscriptions.json";
    val metaProductsDataPath="/usr/local/recommend/meta_Magazine_Subscriptions.json"

    def main(args: Array[String]): Unit = {
        // 创建一个SparkConf配置
        val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(UserSparkConf.cores)
        // 创建一个SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // read reviewData
        val reviewDF = spark.read.json(reviewsDataPath)

        // read product data
        val productDF = spark.read.json(metaProductsDataPath)

        //store data to mongo
        //将当前数据写入到MongoDB
        reviewDF
          .write
          .option("uri",UserMongoDBConf.uri)
          .option("collection",UserMongoDBConf.reviewCollection)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        productDF
          .write
          .option("uri",UserMongoDBConf.uri)
          .option("collection",UserMongoDBConf.productCollection)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        // close spark
        spark.close()
    }


}