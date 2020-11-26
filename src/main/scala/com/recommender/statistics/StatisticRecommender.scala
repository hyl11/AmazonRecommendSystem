package com.recommender.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.recommender.configs.{UserMongoDBConf, UserSparkConf}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object StatisticRecommender {
    def main(args: Array[String]): Unit = {
        //创建SparkConf配置
        val sparkConf = new SparkConf().setAppName("StatisticRecommender").setMaster(UserSparkConf.cores)
        //创建SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        //数据加载进来
        val reviewDF = spark
          .read
          .option("uri",UserMongoDBConf.uri)
          .option("collection",UserMongoDBConf.reviewCollection)
          .format("com.mongodb.spark.sql")
          .load()
          .toDF()
        //创建一张名叫reviews的表
        reviewDF.createOrReplaceTempView("reviews")

        /*----------------------  1  ----------------------------*/
        //统计所有历史数据中每个商品的评分数
        //数据结构 -》  asin,count
        val mostPopularProductsDF = spark.sql(
            "select asin, count(asin) as count from reviews group by asin "
            )
        storeDFInMongoDB(mostPopularProductsDF, UserMongoDBConf.mostPopularProductsCollection)

        /*----------------------  2  ----------------------------*/
        // 近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId, count, yearmonth
        // 创建一个日期格式化工具
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")
        // 注册UDF，将timestamp转化为年月格式yyyyMM
        spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)
        // 把原始rating数据转换成想要的结构productId, score, yearmonth
        val reviewsOfTimeDF = spark.sql(
            "select asin, overall, changeDate(unixReviewTime) as yearmonth from reviews"
            )
        reviewsOfTimeDF.createOrReplaceTempView("reviewOfMonth")
        val mostRecentProductsDF = spark.sql(
            "select asin, count(asin) as count, yearmonth from reviewOfMonth group by yearmonth, " +
              "asin order by yearmonth desc, count desc"
            )
        // 把df保存到mongodb
        storeDFInMongoDB(reviewsOfTimeDF, UserMongoDBConf.mostRecentProductsCollection)

        /*----------------------  3  ----------------------------*/
        // 3. 优质商品统计，商品的平均评分，productId，avg
        val averageProductsDF = spark.sql("select asin, avg(overall) as avg from reviews group by asin order by avg desc")
        storeDFInMongoDB(averageProductsDF, UserMongoDBConf.averageProductsCollection)

        spark.stop()
    }
    def storeDFInMongoDB(df: DataFrame, collection_name: String): Unit ={
        df.write
          .option("uri", UserMongoDBConf.uri)
          .option("collection", collection_name)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()
    }
}
