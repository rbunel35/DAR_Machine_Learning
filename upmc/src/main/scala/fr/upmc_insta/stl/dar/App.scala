package fr.upmc_insta.stl.dar

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.functions._


// cp /mnt/c/Users/ricar/IdeaProjects/upmc/out/artifacts/upmc_jar/upmc.jar .
// spark-submit --class fr.upmc_insta.stl.dar.SparkTest --master local upmc.jar

/**
  * Spark test: calculating airline delays
  */
object SparkTest {

  def readCsv(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def main (arg: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("ATP_Results")
      .getOrCreate()
    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("Error")
    // Example 1 (RDD replaced by Dataset API of Spark 2)
    val dataFolder = "/home/bunel/DAR/"
    val tennisPath = dataFolder + "ATP.csv"
    //val matches = sparkSession.read.textFile(tennisPath)
    val matches = readCsv(sparkSession, tennisPath)
    println("lines in " + tennisPath + ": " + matches.count)


    var matchs = matches.filter($"w_ace".isNotNull && $"l_ace".isNotNull && $"winner_ht".isNotNull && $"loser_ht".isNotNull)
    var avg2 = matchs.groupBy("winner_name").avg("w_ace").withColumnRenamed("avg(w_ace)","avg_aces").withColumnRenamed("winner_name","w_name").join(matchs, $"winner_name" === $"w_name")
    var avg = avg2.withColumn("w_ace", 'w_ace.cast("Double")).withColumn("loser_ht",'loser_ht.cast("Double")).withColumn("winner_ht",'winner_ht.cast("Double"))
    model1(avg)
    model2(avg)
    avg=avg.filter($"winner_rank".isNotNull && $"loser_rank".isNotNull)
    model3(avg)
    model4(avg)
    model5(avg)
    var avg3 = matchs.groupBy("winner_name","surface").avg("w_ace").withColumnRenamed("avg(w_ace)","avg_aces_surface").withColumnRenamed("winner_name","w_name").withColumnRenamed("surface","terrain").join(matchs,$"winner_name" === $"w_name" && $"surface" === $"terrain")
    avg = avg3.withColumn("w_ace", 'w_ace.cast("Double"))
    avg=avg.filter($"winner_rank".isNotNull && $"loser_rank".isNotNull)
    model6(avg)
    model7(avg)
  }

  def model1(dataFrame: DataFrame): Unit ={
    val numericCols = Array("avg_aces")
    runTrainingAndEstimation(dataFrame, "w_ace", numericCols,0.8,0.2,"A.csv")
  }

  def model2(dataFrame: DataFrame): Unit ={
    val numericCols = Array("avg_aces","loser_ht","winner_ht")
    runTrainingAndEstimation(dataFrame, "w_ace", numericCols,0.8,0.2,"B.csv")
  }

  def model3(dataFrame: DataFrame): Unit ={
    val numericCols = Array("avg_aces","loser_ht","winner_ht","winner_rank","loser_rank")
    val categoriCols = Array("surface")
    runTrainingAndEstimation(dataFrame, "w_ace", numericCols,0.8,0.2,"C.csv",categoriCols)
  }

  def model4(dataFrame: DataFrame): Unit ={
    val numericCols = Array("avg_aces","loser_ht","winner_ht","winner_rank","loser_rank")
    val categoriCols = Array("surface")
    runTrainingAndEstimation(dataFrame, "w_ace", numericCols,0.9,0.1,"D.csv",categoriCols)
  }

  def model5(dataFrame: DataFrame): Unit ={
    val numericCols = Array("avg_aces","winner_rank","loser_rank")
    val categoriCols = Array("surface")
    runTrainingAndEstimation(dataFrame, "w_ace", numericCols,0.8,0.2,"E.csv",categoriCols)
  }

  def model6(dataFrame: DataFrame): Unit ={
    val numericCols = Array("avg_aces_surface","winner_rank","loser_rank","loser_ht","winner_ht")
    val categoriCols = Array("surface")
    runTrainingAndEstimation(dataFrame, "w_ace", numericCols,0.9,0.1,"F.csv",categoriCols)
  }

  def model7(dataFrame: DataFrame): Unit ={
    val numericCols = Array("avg_aces_surface","winner_rank","loser_rank","loser_ht","winner_ht")
    val categoriCols = Array("surface")
    runTrainingAndEstimation(dataFrame, "w_ace", numericCols,0.8,0.2,"G.csv",categoriCols)
  }

  def runTrainingAndEstimation(data: DataFrame, labelCol: String, numFeatCols: Array[String],trainnum:Double,testnum:Double,file:String,
                               categFeatCols: Array[String] = Array()): DataFrame = {

    val lr = new LinearRegression().setLabelCol(labelCol)
    val assembler = new VectorAssembler()
    val pipeline = new Pipeline()

    if (categFeatCols.length == 0) {

      assembler
        .setInputCols(numFeatCols)
        .setOutputCol("features")
      pipeline.setStages(Array(assembler, lr))

    } else {

      var featureCols = numFeatCols
      val indexers = categFeatCols.map(c =>
        new StringIndexer().setInputCol(c).setOutputCol(s"${c}_idx")
      )
      val encoders = categFeatCols.map(c => {
        val outputCol = s"${c}_enc"
        featureCols = featureCols :+ outputCol
        new OneHotEncoder().setInputCol(s"${c}_idx").setOutputCol(outputCol)
      })
      assembler
        .setInputCols(featureCols)
        .setOutputCol("features")
      pipeline.setStages(indexers ++ encoders ++ Array(assembler, lr))
    }

    val Array(trainSet, testSet) = data
      .randomSplit(Array(trainnum, testnum))

    // Entrainement du modèle sur trainSet
    val modelLR = pipeline.fit(trainSet)

    // Prédiction sur testSet
    val predictions = modelLR.transform(testSet)
    //predictions.select("prediction", labelCol,"winner_name","loser_name","avg_aces").show(Integer.MAX_VALUE)
    predictions.select("prediction", labelCol,"winner_name","loser_name","loser_ht","winner_ht",numFeatCols(0),"winner_rank","loser_rank","surface").write.format("com.databricks.spark.csv").option("header",true).save(file)
    //predictions.show()
    val predictionsAndObservations = predictions
      .select("prediction", labelCol)
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new RegressionMetrics(predictionsAndObservations)
    val rmse = metrics.rootMeanSquaredError
    println("RMSE: " + rmse)

    predictions
  }



}