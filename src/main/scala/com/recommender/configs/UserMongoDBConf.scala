package com.recommender.configs

object UserMongoDBConf {
    val uri : String = "mongodb://localhost:27017/recommend"

    val reviewCollection : String = "reviews"
    val productCollection : String = "products"
    val mostPopularProductsCollection = "mostPopularProducts"
    val averageProductsCollection = "averageProducts"
    val userRecs = "userRecs"
    val productRecs = "productRecs"
    val onlineRecs = "onlineRecs"
}
