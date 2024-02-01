import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, count, when, sum}


object SparkAssignment1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkAssignment1")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val filePath = "train.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
    // 1）
    val averageFares = df.groupBy("Pclass")
      .agg(avg("Fare").alias("average_fare"))
    averageFares.show()

    // 2)
    val survivalRate = df.groupBy("Pclass")
      .agg((functions.sum("Survived") / functions.count("Survived")).alias("survival_rate"))
    survivalRate.show()
    val maxSurvivalPclass = survivalRate.orderBy("survival_rate").collect()(2).getAs[Int]("Pclass")
    println(s"the max survivalrate class is ${maxSurvivalPclass}")

    // 3)
    val count_Rose = df.filter("Age = 17")
      .filter("Sex= 'female'")
      .filter("SibSp = 1")
      .filter("Pclass = 1")
      .count()
    println(s"there is ${count_Rose} person might be Rose")

    // 4)
    val count_Jack = df.filter("Age = 19 or Age = 20")
      .filter("Sex= 'male'")
      .filter("SibSp = 0")
      .filter("Pclass = 3")
      .count()
    println(s"there is ${count_Jack} person might be Jack")

    // 5)
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
  }
}
