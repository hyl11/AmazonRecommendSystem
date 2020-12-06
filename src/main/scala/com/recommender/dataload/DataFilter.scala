//val reviewDF = spark.read.option("uri", "mongodb://vm2:27017/recommend").option("collection", "reviews").format("com.mongodb.spark.sql").load().toDF()
//val productDF = spark.read.option("uri", "mongodb://vm2:27017/recommend").option("collection", "products").format("com.mongodb.spark.sql").load().toDF()
//
//val filterProductDF = productDF.na.drop().where("title <> ''")
//val allProductAsin = filterProductDF.select("asin", "title")
//
//val filterReviewDF = reviewDF.join(allProductAsin, Seq("asin"), "left").na.drop().where("title <> ''").select("reviewerID", "overall", "asin")
//
//filterProductDF.write.option("uri", "mongodb://vm2:27017/recommend").option("collection", "reviews").mode("overwrite").format("com.mongodb.spark.sql").save()
//filterProductDF.write.option("uri", "mongodb://vm2:27017/recommend").option("collection", "products").mode("overwrite").format("com.mongodb.spark.sql").save()
