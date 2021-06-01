package com.aura.recommender

import breeze.numerics.sqrt
import com.aura.recommender.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author:panghu
  * Date:2021-06-01
  * Description: 通过测试，选取ALS算法最优参数
  */
object ALSTrainer {

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
                .map(rating => Rating(rating.userId, rating.productId, rating.score))
                //后面计算多次用到此RDD
                .cache()

        //切分训练数据集和测试数据集
        val splitArr: Array[RDD[Rating]] = productRatingRDD.randomSplit(Array(0.8, 0.2), 100L)
        val trainData: RDD[Rating] = splitArr(0)
        val testData: RDD[Rating] = splitArr(1)

        //取得最优参数
        val adjustParam: (Int, Double, Double) = adjustALSParam(trainData, testData)
        println(adjustParam._1,adjustParam._2,adjustParam._3)


        spark.stop()
    }


    /**
      * 计算ALS算法最优参数
      *
      * @param trainData
      * @param testData
      */
    def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]) = {
        //指定多个超参值，迭代次数可以指定为固定值
        val res: Array[(Int, Double, Double)] = for (rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
            //yield将每一次循环的结果返回
            yield {
                val model: MatrixFactorizationModel = ALS.train(trainData, rank,5,lambda)
                //获取每一次循环的RMSE
                val rmse: Double = getRMSE(model, testData)
                (rank,lambda,rmse)
            }
        res.sortWith(_._3<_._3).head

    }

    /**
      * 计算均方根误差
      * @param model
      * @param testData
      * @return
      */
    def getRMSE(model: MatrixFactorizationModel, testData: RDD[Rating]): Double = {
        val userProduct: RDD[(Int, Int)] = testData.map(rating => (rating.user, rating.product))
        val predictRating: RDD[Rating] = model.predict(userProduct)
        //用户对商品的真实评分
        val realRating: RDD[((Int, Int), Double)] = testData.map(rating => ((rating.user,rating.product),rating.rating))
        //用户对商品的预测评分
        val predRating: RDD[((Int, Int), Double)] = predictRating.map(rating => ((rating.user,rating.product),rating.rating))

        //计算均方根误差
        val userProductRating: RDD[((Int, Int), (Double, Double))] = realRating.join(predRating)
        val rmse: Double = sqrt(
            userProductRating.map {
                case (userProductId, rating) => {
                    val err = rating._1 - rating._2
                    err * err
                }
            }.mean()
        )
        rmse

    }
}
