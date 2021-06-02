package com.aura.recommender

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author:panghu
  * Date:2021-06-02
  * Description: 基于物品的协同过滤推荐（Item-CF）
  */
case class ProductRatings(userId: Int, productId: Int, score: Double, timeStamp: Long)

case class MongoConfig(uri: String, db: String)

//标准推荐对象
case class Recommendation(productId: Int, score: Double)

//物品相似度（物品推荐列表）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object ItemCFRecommerder {
    //加载数据的表
    val MONGODB_RATING_COLLECTION = "Rating"

    //存储推荐列表的表
    val ITEM_CF_RECS = "ItemCFRecs"
    //推荐列表长度
    val MAX_RECOMMENDATION = 10

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(config("spark.cores"))
        val spark = SparkSession.builder().config(conf).getOrCreate()
        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        import spark.implicits._

        //加载数据
        val productRating = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[ProductRatings]
                .map(rating => (rating.userId, rating.productId, rating.score))
                .toDF("userId", "productId", "rating")
                .cache()

        //TODO:同现相似度算法实现
        //统计每个商品的评分次数
        val numRatersPerProduct: DataFrame = productRating.groupBy("productId").count()
        //将每个商品的评分次数并入到之前的数据中
        val ratingWithCountDF: DataFrame = productRating.join(numRatersPerProduct, "productId")

        //根据userId自连接
        val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
                .toDF("userId", "product1", "rating1", "count1", "product2", "rating2", "count2")
                .select("userId", "product1", "count1", "product2", "count2")
        joinedDF.createOrReplaceTempView("joined")
        //按照两个不同的productId分组，统计不同商品同时被评价的次数
        val cooccurrenceDF = spark.sql(
            """
              |select product1
              |, product2
              |, count(userId) as coocount
              |, first(count1) as count1
              |, first(count2) as count2
              |from joined
              |group by product1, product2
            """.stripMargin
        ).cache()


        val itemCFRecs =  cooccurrenceDF.map(
            row => {
                //取出每个商品被评价的次数和被同一个用户评价的次数，计算同现相似度
                val coocSim = cooccurrenceSim(row.getAs[Long]("coocount"),
                    row.getAs[Long]("count1"),
                    row.getAs[Long]("count2"))
                // (物品1id,(物品2id,相似度评分))
                (row.getAs[Int]("product1"),
                        (row.getAs[Int]("product2"), coocSim))
            }
        )
            .rdd
            .groupByKey()
            .map {
                case (productId, recs) => {
                    recs.toList
                            //过滤掉商品自己
                                    .filter(x => x._1 != productId)
                            .sortWith(_._2>_._2)
                            .map(x => Recommendation(x._1,x._2))
                            .take(MAX_RECOMMENDATION)
                }
            }.toDF()

        //存储到mongodb
        itemCFRecs.write
                .option("uri", mongoConfig.uri)
                .option("collection", ITEM_CF_RECS)
                .format("com.mongodb.spark.sql")
                .mode("overwrite")
                .save()



        spark.stop()
    }

    def cooccurrenceSim(cocount: Long, count1: Long, count2: Long):Double = {
        cocount / Math.sqrt(count1 * count2)
    }
}
