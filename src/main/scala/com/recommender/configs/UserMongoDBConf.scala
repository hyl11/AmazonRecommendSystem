package com.recommender.configs

object UserMongoDBConf {
    val uri : String = "mongodb://localhost:27017/recommend"
    val reviewCollection : String = "reviews"
    val productCollection : String = "products"
    val rateMostProductsCollection = "ratemostproducts"
    val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
    val AVERAGE_PRODUCTS = "AverageProducts"

}
