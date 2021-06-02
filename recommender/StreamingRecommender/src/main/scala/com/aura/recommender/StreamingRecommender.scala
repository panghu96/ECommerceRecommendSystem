package com.aura.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import java.util
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Author:panghu
  * Date:2021-06-01
  * Description: 
  */
// 连接助手对象
object ConnHelper extends Serializable {
    lazy val jedis = new Jedis("hadoop102")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://hadoop102:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

// 标准推荐
case class Recommendation(productId: Int, score: Double)

// 用户的推荐
case class UserRecs(userId: Int, recs: Seq[Recommendation])

//商品的相似度
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object StreamingRecommender {
    //最近评分数量
    val MAX_USER_RATINGS_NUM = 20
    //相似物品数量
    val MAX_SIM_PRODUCTS_NUM = 20
    //操作的表
    //实时推荐表
    val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
    //用户评分表
    val MONGODB_RATING_COLLECTION = "Rating"
    //物品相似度矩阵
    val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
            "mongo.db" -> "recommender",
            "kafka.topic" -> "recommender"
        )
        val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(config("spark.cores"))
        val spark = SparkSession.builder().config(conf).getOrCreate()
        val sc: SparkContext = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(2))

        implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        import spark.implicits._

        //加载物品相似矩阵并广播
        val productRecsRDD: RDD[ProductRecs] = spark.read
                .option("uri", mongoConfig.uri)
                .option("collection", MONGODB_PRODUCT_RECS_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[ProductRecs]
                .rdd
        //方便后面的计算，将RDD转化为Map[productId,Map[productId,score]]
        val productRecsMap: collection.Map[Int, Map[Int, Double]] = productRecsRDD.map(
            item => {
                (item.productId, item.recs.map(recomm => (recomm.productId, recomm.score)).toMap)
            }
        ).collectAsMap()
        //广播
        val simProductsMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(productRecsMap)

        //配置kafka连接参数
        val kafkaPara = Map(
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "recommender",
            "auto.offset.reset" -> "latest"
        )
        //消费kafka中数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")),
                kafkaPara))

        // 用户id|物品id|评分|时间戳
        // 产生评分流
        val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaDStream.map { case msg =>
            println("rating data coming!")
            var attr = msg.value().split("\\|")
            (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
        }

        //TODO:核心实时推荐算法
        ratingStream.foreachRDD(
            rdd => rdd.foreach {
                case (userId, productId, score, timestamp) => {
                    //1.加载redis最近k次的商品评分，Array[(productId,score)]
                    val userRecentlyRatings = getUserRecentlyRating(userId, MAX_USER_RATINGS_NUM, ConnHelper.jedis)
                    //2.当前商品最相似的k个商品，Array[productId]
                    val simProducts = getSimProducts(MAX_SIM_PRODUCTS_NUM,
                        userId,
                        productId,
                        MONGODB_RATING_COLLECTION,
                        simProductsMatrixBroadCast.value
                    )
                    //3.计算备选商品的推荐优先级 Array[(productId,score)]
                    val streamRecs = computeProductScores(userRecentlyRatings,
                        simProducts,
                        simProductsMatrixBroadCast.value)

                    //4.保存数据
                    saveRecsToMongoDB(userId, streamRecs)
                }
            }
        )

        ssc.start()
        ssc.awaitTermination()


    }

    import scala.collection.JavaConversions._

    /**
      * 获取redis中最近k次的商品评分
      *
      * @param userId 当前用户
      * @param num    最近k次
      * @param jedis  jedis连接客户端
      * @return Array[(productId,score)]
      */
    def getUserRecentlyRating(userId: Int, num: Int, jedis: Jedis): Array[(Int, Double)] = {
        //redis list存储格式： 键userId:USERID的值   值PRODUCTID:SCORE
        val productRatingList: util.List[String] = jedis.lrange("userId:" + userId.toString, 0, num)
        val userRecentlyRatingArr: Array[(Int, Double)] = productRatingList.map(
            items => {
                val splits: Array[String] = items.split("\\:")
                (splits(0).trim.toInt, splits(1).trim.toDouble)
            }
        ).toArray
        userRecentlyRatingArr
    }

    /**
      * 获取当前商品最相似的k个商品，并排除用户已经评价过的商品
      *
      * @param num               k的数量
      * @param userId            用户id
      * @param productId         商品id
      * @param productRating     用户评分表
      * @param simProductsMatrix 物品相似度矩阵，外层是可变Map，内层是不可变Map
      * @return Array[productId]
      */
    def getSimProducts(num: Int,
                       userId: Int,
                       productId: Int,
                       productRating: String,
                       simProductsMatrix: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                      (implicit mongoConfig: MongoConfig): Array[Int] = {
        //获取当前商品最相似的k个商品
        val simpleProducts = simProductsMatrix.get(productId).get.toArray
        //用户已经评分过的商品
        val ratingedProducts: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(productRating)
                .find(MongoDBObject("userId" -> userId))
                .toArray.map(item => item.get("productId").toString.toInt)
        //过滤掉用户已经评分过的商品，按照评分排序，取前n个
        val simProducts: Array[Int] = simpleProducts.filter(item => !ratingedProducts.contains(item._1))
                .sortWith(_._2 > _._2).take(num).map(_._1)
        simProducts
    }

    /**
      * 计算物品的推荐优先级
      *
      * @param userRecentlyRatings 用户最近的评分
      * @param simProducts         当前商品的相似商品（备选商品）
      * @param simProductsMatrix   物品相似度矩阵
      * @return Array[(productId,score)]
      */
    def computeProductScores(userRecentlyRatings: Array[(Int, Double)],
                             simProducts: Array[Int],
                             simProductsMatrix: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
    : Array[(Int, Double)] = {
        //保存每个备选商品的推荐优先级评分
        val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
        //保存奖励项数量
        val incrMap = scala.collection.mutable.HashMap[Int, Int]()
        //保存惩罚项数量
        val decrMap = scala.collection.mutable.HashMap[Int, Int]()

        //计算每个备选商品和最近评分的每个商品的优先级评分
        for (userRecentlyRating <- userRecentlyRatings; simProduct <- simProducts) {
            //每个备选商品和最近每个评分过的商品的相似度
            val simScore: Double = getProductsSimScore(userRecentlyRating._1, simProduct, simProductsMatrix)
            //物品相似度大于0.6的才保留
            if (simScore > 0.6) {
                score += ((simProduct, simScore * userRecentlyRating._2))
                //最近评分的商品，评分大于3，属于奖励项
                if (userRecentlyRating._2 > 3) {
                    incrMap(simProduct) = incrMap.getOrDefault(simProduct, 0) + 1
                } else {
                    decrMap(simProduct) = decrMap.getOrDefault(simProduct, 0) + 1
                }
            }
        }

        //按照公式，计算备选商品评分计算推荐优先级，并排序
        score.groupBy(_._1).map {
            case (productId, scoreBuffer) => {
                val recommenScore: Double = scoreBuffer.map(_._2).sum / scoreBuffer.length +
                        log(incrMap.getOrDefault(productId, 1)) -
                        log(decrMap.getOrDefault(productId, 1))

                (productId,recommenScore)
            }
        }.toArray.sortWith(_._2 > _._2)
    }

    /**
      * 备选商品和最近每个评分过的商品的相似度
      *
      * @param userRecentlyRatingId 最近评分过的商品id
      * @param simProductId         备选商品id
      * @param simProductsMatrix    相似度矩阵
      * @return 相似度评分
      */
    def getProductsSimScore(userRecentlyRatingId: Int,
                            simProductId: Int,
                            simProductsMatrix: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
        simProductsMatrix.get(userRecentlyRatingId) match {
            case Some(sim) => sim.get(simProductId) match {
                case Some(score) => score
                case None => 0.0
            }
            case None => 0.0
        }
    }

    /**
      * 换底公式，把e为底的对数换成以10为底
      * @param num
      * @return
      */
    def log(num: Int): Double = {
        val N = 10
        Math.log(num) / Math.log(N)
    }

    /**
      * 保存数据到数据库
      * @param userId
      * @param streamRecs
      */
    def saveRecsToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
        //到StreamRecs的连接
        val streaRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

        streaRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
        streaRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" ->
                streamRecs.map( x => MongoDBObject("productId"->x._1,"score"->x._2)) ))

    }
}
