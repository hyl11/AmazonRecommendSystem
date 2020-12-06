package com.recommender.dataload


import org.apache.spark.sql.{DataFrame, SparkSession}
import com.recommender.configs.UserSparkConf
import com.recommender.configs.UserMongoDBConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit

object DataLoader {

    val music_reviews_path = "/usr/local/recommend/data/Musical_Instruments_5.json"
    val music_path = "/usr/local/recommend/data/meta_Musical_Instruments.json"
    val luxury_path = "/usr/local/recommend/data/meta_Luxury_Beauty.json"
    val luxury_reviews_path = "/usr/local/recommend/data/Luxury_Beauty_5.json"
    val video_path = "/usr/local/recommend/data/meta_Video_Games.json"
    val video_reviews_path = "/usr/local/recommend/data/Video_Games_5.json"

    def main(args: Array[String]): Unit = {
//        // 创建一个SparkConf配置
//        val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(UserSparkConf.cores)
//        // 创建一个SparkSession
//        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        /*  for submit run */
        val spark = SparkSession
          .builder
          .appName("DataLoader")
          .getOrCreate()

        // read cellphoneData
        val musicReviewsDF = spark.read.json(music_reviews_path).select("reviewerID", "overall", "asin")
        val musicDF = spark.read.json(music_path).select("asin", "image", "title", "description")
        val musicFilterDF = getDF(musicReviewsDF, musicDF, "music instruments")

        // read videos
        val videoReviewsDF = spark.read.json(video_reviews_path).select("reviewerID", "overall", "asin")
        val videoDF = spark.read.json(video_path).select("asin", "image", "title", "description")
        val videoFilterDF = getDF(videoReviewsDF, videoDF, "video game")

        // read luxury
        val luxuryReviewsDF = spark.read.json(luxury_reviews_path).select("reviewerID", "overall", "asin")
        val luxuryDF = spark.read.json(luxury_path).select("asin", "image", "title", "description")
        val luxuryFilterDF = getDF(luxuryReviewsDF, luxuryDF, "luxury")

        // merge df
        val reviewDF = musicReviewsDF.union(videoReviewsDF).union(luxuryReviewsDF)
        val productDF = musicFilterDF.union(videoFilterDF).union(luxuryFilterDF)

        //store data to mongo
        //将当前数据写入到MongoDB
        reviewDF
          .write
          .option("uri", UserMongoDBConf.uri)
          .option("collection", UserMongoDBConf.reviewCollection)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        productDF
          .write
          .option("uri", UserMongoDBConf.uri)
          .option("collection", UserMongoDBConf.productCollection)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()
        print("all work is done")
        // close spark
        spark.close()
    }

    def getDF(reviewDF: DataFrame, productDF: DataFrame, product_type: String): DataFrame = {
        val asinDF = reviewDF.select("asin").distinct()
        val filterDF = asinDF.join(productDF, Seq("asin"), "left").distinct()
        filterDF.withColumn("product_type", lit(product_type))
    }
}