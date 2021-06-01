package com.aura.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author:panghu
  * Date:2021-05-31
  * Description: 加载数据到mongodb
  */

/** 分隔符^
  * 3982                            商品id
  * Fuhlen 富勒                      商品名称
  * 1057,439,736                    分类id，不需要
  * B009EJN4T2                      亚马逊id，不需要
  * https://images-cn-4             图片url
  * 外设产品|鼠标|电脑/办公           分类
  * 富勒|鼠标|电子产品|好用|外观漂亮   用户打的UGC标签
  **/
case class Product(productId: Int, productName: String, imageUrl: String, categorys: String, tags: String)

/** 分隔符 逗号
  * 4867,457976,5.0,1395676800
  * 用户id，商品id，评分，时间戳
  */
case class Ratings(userId: Int, productId: Int, score: Double, timeStamp: Long)

case class MongoConfig(uri: String, db: String)

object DataLoader {
    //定义数据文件路径
    val PRODUCT_DATA_PATH = "D:\\develop\\workspace\\bigdata2021\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
    val RATINGS_DATA_PATH = "D:\\develop\\workspace\\bigdata2021\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

    //定义mongodb表名
    val MONGODB_PRODUCT_COLLECTION = "Product"
    val MONGODB_RATING_COLLECTION = "Rating"


    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(config("spark.cores"))
        val spark = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._

        //加载数据
        val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
        val ratingRDD = spark.sparkContext.textFile(RATINGS_DATA_PATH)

        //处理数据
        val produceDF = productRDD.map(
            line => {
                val splits = line.split("\\^")
                Product(splits(0).toInt, splits(1).trim, splits(4).trim, splits(5).trim, splits(6).trim)
            }
        ).toDF()

        val ratingDF = ratingRDD.map(
            line => {
                val splits = line.split(",")
                Ratings(splits(0).toInt, splits(1).toInt, splits(2).toDouble, splits(3).toLong)
            }
        ).toDF()

        //声明一个隐式配置对象
        implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        //保存数据到mongodb
        storeDataInMongoDB(produceDF, ratingDF)


        spark.stop()

    }

    /**
      * 存储数据到mongodb
      *
      * @param produceDF
      * @param ratingDF
      * @param mongoConfig
      */
    def storeDataInMongoDB(produceDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
        //创建mongodb连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

        //通过mongodb客户端拿到表操作对象
        val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
        val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

        //如果表存在，则删除
        productCollection.drop()
        ratingCollection.drop()

        //向数据库中写入数据
        produceDF
                .write
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_PRODUCT_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()
        ratingDF
                .write
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //对表建索引
        productCollection.createIndex(MongoDBObject("productId" -> 1))
        ratingCollection.createIndex(MongoDBObject("userId" -> 1))
        ratingCollection.createIndex(MongoDBObject("productId" -> 1))

        mongoClient.close()
    }
}
