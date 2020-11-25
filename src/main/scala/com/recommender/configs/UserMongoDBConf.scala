package com.recommender.configs

object UserMongoDBConf {
    val uri : String = "mongodb://localhost:27017/recommend"
    val reviewCollection : String = "reviews"
    val productCollection : String = "products"

}
