package com.aura.recommender

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
  * Author:panghu
  * Date:2021-06-02
  * Description: 基于内容CB的推荐 TF-IDF算法实现
  */
case class Product(productId: Int, productName: String, imageUrl: String, categorys: String, tags: String)

case class MongoConfig(uri: String, db: String)

//标准推荐对象
case class Recommendation(productId: Int, score: Double)

//用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

//物品相似度（物品推荐列表）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object CommenBaseRecommerder {
    //加载数据的表
    val MONGODB_PRODUCT_COLLECTION = "Product"

    //存储推荐列表的表
    val COMMEN_BASE_RECS = "CommenRecs" //物品相似度矩阵
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
                .option("collection", MONGODB_PRODUCT_COLLECTION)
                .format("com.mongodb.spark.sql")
                .load()
                .as[Product]
                .map(item => {
                    //分词器默认按照空格分
                    val tags: String = item.tags.map(c => if (c == '|') ' ' else c)
                    (item.productId, item.productName, tags)
                })
                //后面计算多次用到此RDD
                .toDF("productId", "name", "tags").cache()

        //实例化分词器
        val tokenizer: Tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
        //用分词器做转换，将输入列转换为输出列，并加在最后
        val wordsData: DataFrame = tokenizer.transform(productRatingRDD)
        //wordsData.show()

        //定义HashingTF工具，将分词转换为特征稀疏矩阵  setNumFeatures设置特征数量（hash分桶数）
        val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
        //转换
        val featurizedData: DataFrame = hashingTF.transform(wordsData)
        //(800,[125,305,502,510,537,538,550,738],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])
        //featurizedData.show(false)

        //定义IDF工具
        val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        //传入词频，得到IDF模型（统计文档）
        val idfModel: IDFModel = idf.fit(featurizedData)
        //用TF-IDF算法得到新的特征矩阵
        val rescaledData: DataFrame = idfModel.transform(featurizedData)

        //在新的特征矩阵种提取物品向量
        val productFeatures: RDD[(Int, DoubleMatrix)] = rescaledData.map {
            case row => {
                (row.getAs[Int]("productId"),
                        row.getAs[SparseVector]("features").toArray)
            }
        }.rdd.map(item => (item._1, new DoubleMatrix(item._2)))

        //计算物品相似度矩阵
        //物品特征矩阵做笛卡尔积
        val productDoubleMatrix = productFeatures.cartesian(productFeatures)
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
                .option("collection", COMMEN_BASE_RECS)
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
