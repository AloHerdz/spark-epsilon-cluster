import java.time.Instant
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector => MLVector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit, rand, sum, when}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

/** Distributed binary classification and model comparison for Epsilon. */
object EpsilonClassifier {
  private val FeatureCount = 2000
  private val InputSchema = StructType(Seq(
    StructField("label", DoubleType, nullable = false),
    StructField("features", SQLDataTypes.VectorType, nullable = false)))

  private final case class Confusion(total: Long, tn: Long, fp: Long, fn: Long, tp: Long) {
    private def ratio(numerator: Long, denominator: Long): Double =
      if (denominator == 0L) 0.0 else numerator.toDouble / denominator.toDouble

    val accuracy: Double = ratio(tp + tn, total)
    val precision: Double = ratio(tp, tp + fp)
    val recall: Double = ratio(tp, tp + fn)
    val f1: Double = if (precision + recall == 0.0) 0.0 else 2.0 * precision * recall / (precision + recall)
  }

  private def usage(): Nothing = {
    System.err.println(
      "Usage: EpsilonClassifier <file:///absolute/path> <partitions> <required-hosts> <expected-rows> <train-fraction> <seed> <max-iterations> <model-output-dir> <libsvm|tsv|parquet>")
    System.exit(2)
    throw new IllegalStateException("unreachable")
  }

  private def asDouble(value: Any): Double = value match {
    case number: java.lang.Number => number.doubleValue()
    case sequence: scala.collection.Seq[_] if sequence.size == 1 => asDouble(sequence.head)
    case array: Array[_] if array.length == 1 => asDouble(array(0))
    case row: Row if row.length == 1 => asDouble(row.get(0))
    case other => throw new IllegalArgumentException(s"Expected a numeric feature, got ${Option(other).map(_.getClass.getName).getOrElse("null")}")
  }

  private def vectorFromNested(value: Any): MLVector = value match {
    case vector: MLVector => vector
    case sequence: scala.collection.Seq[_] => Vectors.dense(sequence.iterator.map(asDouble).toArray)
    case array: Array[_] => Vectors.dense(array.iterator.map(asDouble).toArray)
    case other => throw new IllegalArgumentException(s"Unsupported Epsilon feature container: ${Option(other).map(_.getClass.getName).getOrElse("null")}")
  }

  private def readTsv(spark: SparkSession, path: String, partitions: Int): DataFrame = {
    val rows = spark.sparkContext.textFile(path, partitions).mapPartitions { lines =>
      lines.filter(_.nonEmpty).map { line =>
        val values = line.split("\\t", -1)
        if (values.length != FeatureCount + 1) {
          throw new IllegalArgumentException(s"Expected ${FeatureCount + 1} tab-separated values, got ${values.length}")
        }
        val label = values(0).toDouble
        val features = values.iterator.drop(1).map(_.toDouble).toArray
        if (features.length != FeatureCount || features.exists(v => !java.lang.Double.isFinite(v))) {
          throw new IllegalArgumentException("Epsilon TSV row has invalid feature values")
        }
        Row(label, Vectors.dense(features))
      }
    }
    spark.createDataFrame(rows, InputSchema)
  }

  private def readParquet(spark: SparkSession, path: String): DataFrame = {
    val rows = spark.read.parquet(path).select("label", "inputs").rdd.map { row =>
      val label = asDouble(row.get(0))
      val features = vectorFromNested(row.get(1))
      if (features.size != FeatureCount || features.toArray.exists(v => !java.lang.Double.isFinite(v))) {
        throw new IllegalArgumentException(s"Epsilon Parquet row must have $FeatureCount finite features")
      }
      Row(label, features)
    }
    spark.createDataFrame(rows, InputSchema)
  }

  private def readInput(spark: SparkSession, path: String, format: String, partitions: Int): DataFrame = format match {
    case "libsvm" =>
      spark.read.format("libsvm").option("numFeatures", FeatureCount.toString).load(path)
        .select(col("label").cast("double").as("label"), col("features"))
    case "tsv" => readTsv(spark, path, partitions)
    case "parquet" => readParquet(spark, path)
    case other => throw new IllegalArgumentException(s"Unsupported Epsilon format: $other")
  }

  private def activeExecutorHosts(sc: SparkContext): Seq[String] =
    sc.statusTracker.getExecutorInfos.map(_.host()).distinct.sorted.toSeq

  private def rowsFor(stats: Seq[(String, Boolean, Double, Long)], split: String): Long =
    stats.filter(_._1 == split).map(_._4).sum

  private def rowsForLabel(stats: Seq[(String, Boolean, Double, Long)], split: String, label: Double): Long =
    stats.filter(s => s._1 == split && s._2 && s._3 == label).map(_._4).sum

  private def number(row: Row, name: String): Long =
    Option(row.getAs[java.lang.Number](name)).map(_.longValue()).getOrElse(0L)

  private def confusion(predictions: DataFrame): Confusion = {
    val label = col("label")
    val prediction = col("prediction")
    val summary = predictions.agg(
      count(lit(1)).as("total"),
      sum(when(label === 0.0 && prediction === 0.0, 1L).otherwise(0L)).as("tn"),
      sum(when(label === 0.0 && prediction === 1.0, 1L).otherwise(0L)).as("fp"),
      sum(when(label === 1.0 && prediction === 0.0, 1L).otherwise(0L)).as("fn"),
      sum(when(label === 1.0 && prediction === 1.0, 1L).otherwise(0L)).as("tp")
    ).first()
    Confusion(number(summary, "total"), number(summary, "tn"), number(summary, "fp"),
      number(summary, "fn"), number(summary, "tp"))
  }

  private def saveModel(
      name: String,
      model: Transformer,
      outputRoot: String,
      trainFraction: Double,
      seed: Long,
      maxIterations: Int): String = {
    val (coefficients, intercept, threshold, regularization) = model match {
      case logistic: LogisticRegressionModel =>
        (logistic.coefficients, logistic.intercept, logistic.getThreshold, 0.01)
      case svm: LinearSVCModel =>
        (svm.coefficients, svm.intercept, svm.getThreshold, 0.01)
      case other => throw new IllegalArgumentException(s"Unsupported classifier model: ${other.getClass.getName}")
    }
    val weights = coefficients.toArray
    require(weights.length == FeatureCount && weights.forall(java.lang.Double.isFinite) && java.lang.Double.isFinite(intercept),
      s"$name produced invalid model coefficients")
    val json = s"""{
      |  "model": "$name",
      |  "feature_count": $FeatureCount,
      |  "label_mapping": {"-1": 0, "0": 0, "1": 1},
      |  "intercept": ${java.lang.Double.toString(intercept)},
      |  "threshold": ${java.lang.Double.toString(threshold)},
      |  "regularization": $regularization,
      |  "max_iterations": $maxIterations,
      |  "train_fraction": $trainFraction,
      |  "seed": $seed,
      |  "coefficients": [${weights.map(java.lang.Double.toString).mkString(",")}]
      |}""".stripMargin + "\n"
    val directory = Paths.get(outputRoot).toAbsolutePath
    Files.createDirectories(directory)
    val target = directory.resolve(s"$name.json")
    val temporary = Files.createTempFile(directory, s".$name-", ".tmp")
    Files.writeString(temporary, json, StandardCharsets.UTF_8)
    try Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    catch {
      case _: java.nio.file.AtomicMoveNotSupportedException =>
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
    }
    target.toString
  }

  private def evaluate(
      name: String,
      model: Transformer,
      test: DataFrame,
      outputRoot: String,
      started: Long,
      trainFraction: Double,
      seed: Long,
      maxIterations: Int): Confusion = {
    val predictions = model.transform(test)
      .select(col("label"), col("prediction"), col("rawPrediction"))
      .persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val matrix = confusion(predictions)
      val auc = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("rawPrediction")
        .setMetricName("areaUnderROC")
        .evaluate(predictions)
      val modelPath = saveModel(name, model, outputRoot, trainFraction, seed, maxIterations)
      val elapsed = (System.nanoTime() - started).toDouble / 1e9
      println(f"MODEL name=$name accuracy=${matrix.accuracy}%.6f precision=${matrix.precision}%.6f recall=${matrix.recall}%.6f f1=${matrix.f1}%.6f auc_roc=$auc%.6f test_rows=${matrix.total}%d elapsed_s=$elapsed%.2f path=$modelPath")
      println(s"CONFUSION model=$name tn=${matrix.tn} fp=${matrix.fp} fn=${matrix.fn} tp=${matrix.tp}")
      matrix
    } finally {
      predictions.unpersist(blocking = false)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 9 || !args(0).startsWith("file:///")) usage()
    val input = args(0)
    val partitions = args(1).toInt
    val requiredHosts = args(2).toInt
    val expectedRows = args(3).toLong
    val trainFraction = args(4).toDouble
    val seed = args(5).toLong
    val maxIterations = args(6).toInt
    val modelOutput = args(7)
    val format = args(8)
    if (partitions < 2 || requiredHosts < 1 || expectedRows < 0 ||
        trainFraction <= 0.5 || trainFraction >= 1.0 || maxIterations < 1 ||
        !(format == "libsvm" || format == "tsv" || format == "parquet")) usage()

    val startedAt = Instant.now().toString
    val conf = new SparkConf().setAppName("Epsilon distributed classification comparison")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    var passed = false
    var data: DataFrame = null
    var train: DataFrame = null
    var test: DataFrame = null
    try {
      sc.setLogLevel("WARN")
      val raw = readInput(spark, input, format, partitions)
      // Persist one disk-backed copy of the parsed vectors. The grouped summary
      // below validates labels and counts the split in this same input pass.
      data = raw
        .withColumn("__valid_label", col("label").isin(-1.0, 0.0, 1.0))
        .withColumn("__split", when(rand(seed) < lit(trainFraction), lit("train")).otherwise(lit("test")))
        .withColumn("label", when(col("label") > 0.0, lit(1.0)).otherwise(lit(0.0)))
        .select(col("__split"), col("__valid_label"), col("label"), col("features"))
        .persist(StorageLevel.DISK_ONLY)

      val summaryRows = data.groupBy("__split", "__valid_label", "label")
        .agg(count(lit(1)).as("rows")).collect()
      val stats = summaryRows.map(r => (r.getString(0), r.getBoolean(1), r.getDouble(2), r.getLong(3))).toSeq
      val totalRows = stats.map(_._4).sum
      val invalidRows = stats.filter(!_._2).map(_._4).sum
      require(invalidRows == 0L, s"Epsilon contains $invalidRows invalid labels; expected only -1, 0, or 1")
      val trainRows = rowsFor(stats, "train")
      val testRows = rowsFor(stats, "test")
      require(trainRows > 0 && testRows > 0, "The seeded split produced an empty train or test set")
      require(expectedRows == 0L || totalRows == expectedRows,
        s"Expected $expectedRows Epsilon rows, found $totalRows")
      val trainLabels = Map(0.0 -> rowsForLabel(stats, "train", 0.0), 1.0 -> rowsForLabel(stats, "train", 1.0))
      val testLabels = Map(0.0 -> rowsForLabel(stats, "test", 0.0), 1.0 -> rowsForLabel(stats, "test", 1.0))
      require(trainLabels.values.forall(_ > 0L) && testLabels.values.forall(_ > 0L),
        "Both classes must be represented in train and test splits")

      train = data.filter(col("__split") === "train").select(col("label"), col("features"))
      test = data.filter(col("__split") === "test").select(col("label"), col("features"))
      println(s"DATA format=$format rows=$totalRows features=$FeatureCount train_rows=$trainRows test_rows=$testRows seed=$seed train_fraction=$trainFraction")
      println(s"LABELS train_0=${trainLabels.getOrElse(0.0, 0L)} train_1=${trainLabels.getOrElse(1.0, 0L)} test_0=${testLabels.getOrElse(0.0, 0L)} test_1=${testLabels.getOrElse(1.0, 0L)}")

      val logisticStarted = System.nanoTime()
      val logistic = new LogisticRegression()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(maxIterations)
        .setRegParam(0.01)
        .setElasticNetParam(0.0)
        .setMaxBlockSizeInMB(16.0)
        .fit(train)
      evaluate("logistic-regression", logistic, test, modelOutput, logisticStarted,
        trainFraction, seed, maxIterations)

      val svmStarted = System.nanoTime()
      val svm = new LinearSVC()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxIter(maxIterations)
        .setRegParam(0.01)
        .setMaxBlockSizeInMB(16.0)
        .fit(train)
      evaluate("linear-svc", svm, test, modelOutput, svmStarted,
        trainFraction, seed, maxIterations)

      val hosts = activeExecutorHosts(sc)
      require(hosts.size >= requiredHosts,
        s"Only ${hosts.size} Spark executor host(s) remained active; this run requires $requiredHosts")
      println(s"EXECUTORS hosts=${hosts.mkString(",")}")
      println(s"CLASSIFICATION_RESULT app_id=${sc.applicationId} started=$startedAt rows=$totalRows features=$FeatureCount hosts=${hosts.mkString(",")} models=logistic-regression,linear-svc required_hosts=$requiredHosts")
      passed = true
    } finally {
      if (data != null) data.unpersist(blocking = false)
      spark.stop()
    }
    if (!passed) System.exit(3)
  }
}
