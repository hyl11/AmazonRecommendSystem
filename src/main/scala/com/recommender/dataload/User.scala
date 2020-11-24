package com.recommender.dataload

case class User(reviewerID:String, reviewerName:String, alsoBuy:List[String], alsoViewed:List[String])
