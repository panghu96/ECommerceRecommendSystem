package com.aura.recommender

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author:panghu
  * Date:2021-05-31
  * Description: 离线统计推荐
  * 历史热门商品统计
  * 最近热门商品统计
  * 商品平均得分统计
  */
case class Ratings(userId: Int, productId: Int, score: Double, timeStamp: Long)

case class MongoConfig(uri: String, db: String)


object StatisticsRecommender {
    //加载数据的表
    val MONGODB_RATING_COLLECTION = "Rating"
    //统计的表的名称
    val RATE_MORE_PRODUCTS = "RateMoreProducts"
    val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
    val AVERAGE_PRODUCTS = "AverageProducts"


    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
            "mongo.db" -> "recommender"
        )

        val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(config("spark.cores"))
        val spark = SparkSession.builder().config(conf).getOrCreate()
        implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        import spark.implicits._

        //加载数据
        val ratingDF = spark
                .read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Ratings]
                .toDF()
        //创建临时表
        ratingDF.createOrReplaceTempView("rating")
        //历史热门商品统计 按照评分次数统计
        val rateMoreProductDF: DataFrame = spark.sql("select productId,count(productId) count from rating group by productId order by count desc")

        //最近热门商品统计 时间戳转为yyyyMM格式，统计每个月的评分次数
        val dateFormat = new SimpleDateFormat("yyyyMM")
        spark.udf.register("changeDate",(tm:Long) => dateFormat.format(new Date(tm*1000L)).toLong)
        val yearMonthRating = spark.sql("select productId,score,changeDate(timeStamp) yearMonth from rating")
        yearMonthRating.createOrReplaceTempView("yearmonth_rating")
        val rateMoreRecentDF = spark.sql("select yearMonth,productId,count(productId) count from yearmonth_rating group by yearMonth,productId order by yearMonth desc,count desc")

        //商品平均得分统计
        val avgProductDF = spark.sql("select productId,avg(score) avg from rating group by productId order by avg desc")

        //保存到mongodb

        storeDataInMongoDB(rateMoreProductDF, RATE_MORE_PRODUCTS)
        storeDataInMongoDB(rateMoreRecentDF, RATE_MORE_RECENTLY_PRODUCTS)
        storeDataInMongoDB(avgProductDF, AVERAGE_PRODUCTS)

        spark.stop()
    }

    def storeDataInMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig): Unit = {
        df.write
                .option("uri", mongoConfig.uri)
                .option("collection", collectionName)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
    }

}
