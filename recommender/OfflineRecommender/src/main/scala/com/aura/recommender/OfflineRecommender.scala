package com.aura.recommender

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
  * Author:panghu
  * Date:2021-06-01
  * Description: 基于隐语义模型（LFM）的协同过滤推荐
  */
case class ProductRatings(userId: Int, productId: Int, score: Double, timeStamp: Long)

case class MongoConfig(uri: String, db: String)

//标准推荐对象
case class Recommendation(productId: Int, score: Double)

//用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

//物品相似度（物品推荐列表）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OfflineRecommender {
    //加载数据的表
    val MONGODB_RATING_COLLECTION = "Rating"
    //存储推荐列表的表
    val USER_RECS = "UserRecs" //用户推荐列表
    val PRODUCT_RECS = "ProductRecs" //物品相似度矩阵
    //推荐列表长度
    val USER_MAX_RECOMMENDATION = 20


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
        val productRatingRDD = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_RATING_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[ProductRatings]
                .rdd
                .map(rating => (rating.userId, rating.productId, rating.score))
                //后面计算多次用到此RDD
                .cache()

        //提取用户矩阵和物品矩阵
        val userRDD = productRatingRDD.map(_._1).distinct()
        val productRDD: RDD[Int] = productRatingRDD.map(_._2).distinct()

        //TODO:用户推荐列表
        //数据转为模型需要的RDD[Rating]格式
        val ratingRDD: RDD[Rating] = productRatingRDD.map(rating => Rating(rating._1, rating._2, rating._3))
        //定义超参 rank是隐语义因子个数，iterations是迭代次数，lambda是正则化项系数
        val (rank, iterations, lambda) = (50, 5, 0.01)
        //ALS算法训练模型
        val model: MatrixFactorizationModel = ALS.train(ratingRDD, rank, iterations, lambda)

        //用户和物品矩阵做笛卡尔积，得到用户推荐物品的空矩阵
        val userProducts: RDD[(Int, Int)] = userRDD.cartesian(productRDD)
        //模型预测评分
        val preRatings: RDD[Rating] = model.predict(userProducts)

        //根据用户分组，按照预测评分倒序，取前20
        val userRecs: DataFrame = preRatings
                //过滤掉评分为0的数据
                .filter(_.rating > 0)
                .map(rating => (rating.user, (rating.product, rating.rating)))
                .groupByKey()
                .map {
                    case (userId, recs) => {
                        //排序取前20
                        val productRatingTop: List[(Int, Double)] = recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION)
                        val recommendations: List[Recommendation] = productRatingTop.map(item => Recommendation(item._1, item._2))
                        UserRecs(userId, recommendations)
                    }
                }.toDF()
        //存入mongodb
        userRecs.write
                .option("uri", mongoConfig.uri)
                .option("collection", USER_RECS)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

        //TODO:物品相似度
        //在用户物品评分模型中取出物品特征矩阵
        val productFeatures: RDD[(Int, Array[Double])] = model.productFeatures
        val productFeaturesMatrix: RDD[(Int, DoubleMatrix)] = productFeatures.map {
            case (productId, arr) => {
                (productId, new DoubleMatrix(arr))
            }
        }

        //物品特征矩阵做笛卡尔积
        val productDoubleMatrix: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] = productFeaturesMatrix.cartesian(productFeaturesMatrix)
                //过滤出相同的物品
                .filter {
            case (p, q) => p._1 != q._1
        }
        //计算余弦相似度
        val productSimRDD: RDD[(Int, (Int, Double))] = productDoubleMatrix.map {
            case (p, q) => {
                val sim: Double = consinSim(p._2, q._2)
                //组装数据  (商品1id,(商品2id,相似度))
                (p._1, (q._1, sim))
            }
        }.filter(_._2._2 > 0.6)
        //分组，封装
        val productRecs: DataFrame = productSimRDD.groupByKey().map {
            case (productId, productSimIte) => {
                val productSim: Seq[Recommendation] = productSimIte.toList.map(item => Recommendation(item._1, item._2))
                ProductRecs(productId, productSim)
            }
        }.toDF()
        //存储到mongodb
        productRecs.write
                .option("uri", mongoConfig.uri)
                .option("collection", PRODUCT_RECS)
                .format("com.mongodb.spark.sql")
                .mode("overwrite")
                .save()


        spark.stop()
    }

    /**
      * 计算余弦相似度
      *
      * @param pFeatures
      * @param qFeatures
      * @return
      */
    def consinSim(pFeatures: DoubleMatrix, qFeatures: DoubleMatrix): Double = {
        //第二范数，即向量的模长
        pFeatures.dot(qFeatures) / (pFeatures.norm2() * qFeatures.norm2())
    }
}
