package com.recommend.online
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.util.HashMap

import com.recommender.configs.{UserMongoDBConf, UserSparkConf}
import org.apache.spark._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import java.util.Date
/*
*
* IntID  --- where ---> DF(id, [id, id, id]) ----> (time, [(id, type, description...),()...])
*
* */

// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )
// 定义用户的推荐列表
case class UserRecs( userId: Int, recs: Seq[Recommendation] )
// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )

object OnlineStreaming {
    def main(args: Array[String]): Unit = {

        // 创建spark session
        val sparkConf = new SparkConf().setMaster(UserSparkConf.cores).setAppName("OfflineRecommender")

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val kafkaParams = Map[String, Object]("bootstrap.servers" -> "vm1:9092","key.deserializer" -> classOf[StringDeserializer],"value.deserializer" -> classOf[StringDeserializer],"group.id" -> "Test","auto.offset.reset" -> "latest","enable.auto.commit" -> (false: java.lang.Boolean))

        val topics = Array("recommend")

        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))

        val out = stream.map(record => (record.key, record.value)).map(_._2)
        out.foreachRDD( rdd => {
            rdd.foreach( p => {
                val productRecs = spark
                  .read
                  .option("uri",UserMongoDBConf.uri)
                  .option("collection",UserMongoDBConf.productRecs)
                  .format("com.mongodb.spark.sql")
                  .load()
                  .as[ProductRecs]
                  .where("productId == $p")
                if(productRecs.collect().size != 0){  //insert new recs data
                    val productsWithInt =  spark
                      .read
                      .option("uri",UserMongoDBConf.uri)
                      .option("collection","productsWithInt")
                      .format("com.mongodb.spark.sql")
                      .load()
                    productRecs.first().recs.foreach(id => {
                        productsWithInt.
                          where(s"intAsin == $id").
                          withColumn("time", lit(new Date().getTime))
                          .write
                          .option("uri", UserMongoDBConf.uri)
                          .option("collection", UserMongoDBConf.onlineRecs)
                          .mode("append")
                          .format("com.mongodb.spark.sql")
                          .save()
                    })
                }
            })
        })
        val words = out.map(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()

        ssc.start()
    }

}
