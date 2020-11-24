package com.recommender.dataload

case class Product(asin:String, title:String, feature:List[String], description:String,
                   price:Float, imageUrl:String, also_buy:List[String], also_viewed:List[String],
                   salesRank:String, brand:String, categories:List[String])
