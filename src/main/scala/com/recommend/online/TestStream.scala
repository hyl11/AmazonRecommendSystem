package com.recommend.online
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.util.HashMap

import com.recommender.configs.{UserMongoDBConf, UserSparkConf}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

object TestStream {
    def main(args: Array[String]): Unit = {

        // 创建spark session
        val sparkConf = new SparkConf().setMaster(UserSparkConf.cores).setAppName("OfflineRecommender")

     //   val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val ssc = new StreamingContext(sparkConf, Seconds(10))
        ssc.checkpoint("/usr/local/recommend/tmp")

        val myDStream = ssc.socketTextStream("localhost", 9999)
        val wordAndOneDStream = myDStream.flatMap(_.split(","))
          .map((_, 1))

        val wordCount = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.sum
            val lastCount = state.getOrElse(0)
            Some(currentCount + lastCount)
        }

        val resultDStream: DStream[(String, Int)] = wordAndOneDStream.updateStateByKey(wordCount)
        resultDStream.foreachRDD(rdd => {
            rdd.foreach( p => {
                p._1
            })
        })
        resultDStream.print()
        ssc.start()


    }
}
