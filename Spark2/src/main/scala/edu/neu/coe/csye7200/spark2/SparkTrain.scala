package edu.neu.coe.csye7200.spark2

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkTrain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark2")
      .config("spark.master", "local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    // data process
    val path = "titanic/train.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    val test_path = "titanic/test.csv"
    val df2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(test_path)

    val droppedDf = df.drop("PassengerId", "Name", "Ticket", "Cabin")

    val meanAge = droppedDf.select(mean("Age")).first()(0).asInstanceOf[Double].toInt
    val filledDf = droppedDf.na.fill(meanAge, Seq("Age"))

    val withFamilySizeDf = filledDf.withColumn("FamilySize", col("SibSp") + col("Parch"))
      .drop("SibSp", "Parch")

    val finalDf = withFamilySizeDf.na.drop(Seq("Embarked"))

    val splitdf = finalDf.randomSplit(Array(0.8, 0.2))
    val (train, test) = (splitdf(0), splitdf(1))

    // The model vex could only get int or double as input, so converse these string to Int
    val indexer = new StringIndexer().setInputCol("Sex").setOutputCol("Sex_")
    val indexer2 = new StringIndexer().setInputCol("Embarked").setOutputCol("Embarked_")

    // use Pclass, Sex, Age, Fare, Embarcked, FamilyCount as the features
    val assembler = new VectorAssembler().setInputCols(Array("Pclass", "Sex_", "Age", "Fare", "Embarked_", "FamilySize")).setOutputCol("features")
    val rf = new RandomForestClassifier().setLabelCol("Survived").setFeaturesCol("features")
    val pipeline = new Pipeline().setStages(Array(indexer, indexer2, assembler, rf))

    // training use RandomForestClassifier
    val model = pipeline.fit(train)

    val labelsAndPredictions = model.transform(test)

    labelsAndPredictions.select("prediction", "Survived", "features").show(false)

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("Survived").setRawPredictionCol("prediction")
    val accuracy = evaluator.evaluate(labelsAndPredictions)

    // get the accuracy
    println(s"Accuracy: $accuracy")

  }
}
