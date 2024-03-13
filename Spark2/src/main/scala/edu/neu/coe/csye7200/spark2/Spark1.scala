package edu.neu.coe.csye7200.spark2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, when, sum}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.functions
import org.apache.spark.{SparkConf, SparkContext}
object Spark1 extends App{
//  val conf = new SparkConf().setAppName("Spark2").setMaster("local[*]")
//  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Spark2")
  .config("spark.master", "local[*]")
  .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val path = "titanic/train.csv"
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)
  val path_test = "titanic/test.csv"
  val df_test = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path_test)
//  df.show()




  // 1）
  println("The 1st question:")
  val averageFares = df.groupBy("Pclass")
    .agg(avg("Fare").alias("average_fare"))
  averageFares.show()
  println()

  // 2)
  println("The 2nd question:")
  val survivalRate = df.groupBy("Pclass")
    .agg((functions.sum("Survived") / functions.count("Survived")).alias("survival_rate"))
  survivalRate.show()
  val maxSurvivalPclass = survivalRate.orderBy("survival_rate").collect()(2).getAs[Int]("Pclass")
  println(s"the max survivalrate class is ${maxSurvivalPclass}\n")

  // 3)
  println("The 3rd question:")
  val count_Rose = df.filter("Age = 17")
    .filter("Sex= 'female'")
    .filter("SibSp = 0")
    .filter("Pclass = 1")
    .filter("Parch = 1")
    .count()
  println(s"there is ${count_Rose} person might be Rose\n")

  // 4)
  println("The 4th question:")
  val count_Jack = df.filter("(Age = 19 OR Age = 20) OR Age IS NULL")
    .filter("Sex= 'male'")
    .filter("SibSp = 0")
    .filter("Pclass = 3")
    .count()
  println(s"there is ${count_Jack} person might be Jack\n")

  // 5)
  println("The 5th question:")
  val dfWithAgeGroup = df.withColumn("AgeGroup", when($"Age".isNull, "Unknown")
    .otherwise(
      when($"Age" <= 10, "01-10")
        .otherwise(
          when($"Age" <= 20, "11-20")
            .otherwise(
              when($"Age" <= 30, "21-30")
                .otherwise(
                  when($"Age" <= 40, "31-40")
                    .otherwise(
                      when($"Age" <= 50, "41-50")
                        .otherwise(
                          when($"Age" <= 60, "51-60")
                            .otherwise(
                              when($"Age" <= 70, "61-70")
                                .otherwise(
                                  when($"Age" <= 80, "71-80")
                                    .otherwise("81+")
                                )
                            )
                        )
                    )
                )
            )
        )
    )
  )
  val groupStats = dfWithAgeGroup.groupBy($"AgeGroup")
    .agg(
      avg($"Fare").alias("average_fare"),
      (sum($"Survived") / count($"Survived")).alias("survival_rate")
    )
  groupStats.show()
  val highestSurvivalRateGroup = groupStats.orderBy($"survival_rate".desc).first()
  println("The young and old paid less fare than the middle-aged")
  println(s"Age Group with highest survival rate: ${highestSurvivalRateGroup.getAs[String]("AgeGroup")}")
  spark.stop()
//  sc.stop()









}
