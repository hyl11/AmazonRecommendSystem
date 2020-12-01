package com.recommender.offline

import com.recommender.configs.{UserMongoDBConf, UserSparkConf}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )
// 定义用户的推荐列表
case class UserRecs( userId: Int, recs: Seq[Recommendation] )
// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )

object OfflineRecommender {

    val USER_MAX_RECOMMENDATION = 20

    def main(args: Array[String]): Unit = {

        // 创建spark session
        val sparkConf = new SparkConf().setMaster(UserSparkConf.cores).setAppName("OfflineRecommender")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        //读取mongoDB中的业务数
        val ratingDF = spark
          .read
          .option("uri",UserMongoDBConf.uri)
          .option("collection",UserMongoDBConf.reviewCollection)
          .format("com.mongodb.spark.sql")
          .load()
          .select("reviewerID", "asin", "overall")

        /*   encode string id to int id for als */
        val reviewerIndexer = new StringIndexer().setInputCol("reviewerID").setOutputCol("intReviewerID")
        val partIntDF = reviewerIndexer.fit(ratingDF).transform(ratingDF)
        val productIndexer = new StringIndexer().setInputCol("asin").setOutputCol("intAsin")
        val allIntDF = productIndexer.fit(partIntDF).transform(partIntDF)

        /*  get rdd of data  */
        val ratingRDD = allIntDF.select("intReviewerID", "intAsin", "overall")
          .rdd
          .map(x => (x.getDouble(0).toInt, x.getDouble(1).toInt, x.getDouble(2)))
          .cache()

        //用户的数据集 RDD[Int]
        val userRDD = ratingRDD.map(_._1).distinct()
        val productRDD = ratingRDD.map(_._2).distinct()

        //创建训练数据集
        val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3.toFloat))
        // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是ALS的正则化参
        val (rank,iterations,lambda) = (50, 5, 0.01)
        // 调用ALS算法训练隐语义模型

        val model = ALS.train(trainData,rank,iterations,lambda)

        // 2. 获得预测评分矩阵，得到用户的推荐列表
        // 用userRDD和productRDD做一个笛卡尔积，得到空的userProductsRDD表示的评分矩阵
        val userProducts = userRDD.cartesian(productRDD)
        val preRating = model.predict(userProducts)
        // 从预测评分矩阵中提取得到用户推荐列表
        val userRecs = preRating.filter(_.rating>0)
          .map(
              rating => ( rating.user, ( rating.product, rating.rating ) )
          )
          .groupByKey()
          .map{
              case (userId, recs) =>
                  UserRecs( userId, recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)) )
          }
          .toDF()
        userRecs.write
          .option("uri", UserMongoDBConf.uri)
          .option("collection", UserMongoDBConf.userRecs)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        // 3. 利用商品的特征向量，计算商品的相似度列表
        val productFeatures = model.productFeatures.map{
            case (productId, features) => ( productId, new DoubleMatrix(features) )
        }
        // 两两配对商品，计算余弦相似度
        val productRecs = productFeatures.cartesian(productFeatures)
          .filter{
              case (a, b) => a._1 != b._1
          }
          // 计算余弦相似度
          .map{
              case (a, b) =>
                  val simScore = consinSim( a._2, b._2 )
                  ( a._1, ( b._1, simScore ) )
          }
          .filter(_._2._2 > 0.4)
          .groupByKey()
          .map{
              case (productId, recs) =>
                  ProductRecs( productId, recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)) )
          }
          .toDF()
        productRecs.write
          .option("uri", UserMongoDBConf.uri)
          .option("collection", UserMongoDBConf.productRecs)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        spark.stop()
    }
    def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double ={
        product1.dot(product2)/ ( product1.norm2() * product2.norm2() )
    }
}
