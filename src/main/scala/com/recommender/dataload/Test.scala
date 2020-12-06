//package com.recommender.dataload
//
//
//import org.apache.spark.sql.SparkSession
//import com.recommender.configs.UserSparkConf
//import com.recommender.configs.UserMongoDBConf
//import org.apache.spark.SparkConf
//
//object Test {
//    val test1 = "/usr/local/recommend/test1.json";
//    val test2 = "/usr/local/recommend/meta_Luxury_Beauty.json"
//
//    def main(args: Array[String]): Unit = {
//
//        // 创建一个SparkConf配
//        val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(UserSparkConf.cores)
//        // 创建一个SparkSession
//        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//
//        // read reviewData
//        val reviewDF = spark.read.json(test1)
//
//        // read product data
//        val productDF = spark.read.json(test2)
//        // filter data
//        val asinDF = reviewDF.select("asin").distinct()
//        val filterDF = asinDF.join(productDF, Seq("asin"), "left")
//
//        //store data to mongo
//        //将当前数据写入到MongoDB
//        filterDF.write.format("json").save("/usr/local/recommend/filter.json")
//        //productDF.write.format("json").save("/usr/local/recommend/test2.json")
//
//        // close spark
//        spark.close()
//    }
//
//
//}