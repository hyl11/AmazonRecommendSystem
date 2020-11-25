package com.recommender.dataload

case class Review(imageUrl:String, overall:Float, vote:Int, verified:Boolean, reviewTime:String,
                  reviewerID:String,
                  asin:String, style:Map[String, String], reviewerName:String, reviewText:String,
                  summary:String, unixReviewTime:Long)
