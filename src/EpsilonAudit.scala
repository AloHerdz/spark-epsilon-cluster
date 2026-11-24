import java.net.InetAddress
import java.time.Instant
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/** Distributed, streaming audit of the LIBSVM Epsilon training set. */
object EpsilonAudit {
  private final case class PartitionSummary(
      host: String,
      partition: Int,
      rows: Long,
      positive: Long,
      negative: Long,
      features: Long,
      invalid: Long,
      sumSquares: Double,
      maxIndex: Int) extends Serializable

  private def usage(): Nothing = {
    System.err.println(
      "Usage: EpsilonAudit <file:///absolute/path> [partitions] [hold-seconds] [required-hosts] [expected-rows] [repartition] [libsvm|tsv]")
    System.exit(2)
    throw new IllegalStateException("unreachable")
  }

  private def scanPartition(partition: Int, lines: Iterator[String], holdSeconds: Int, format: String): PartitionSummary = {
    val host = InetAddress.getLocalHost.getHostName
    var rows = 0L
    var positive = 0L
    var negative = 0L
    var features = 0L
    var invalid = 0L
    var sumSquares = 0.0
    var maxIndex = 0

    while (lines.hasNext) {
      val line = lines.next().trim
      if (line.nonEmpty) {
        if (format == "tsv") {
          var from = 0
          var field = 0
          var malformed = false
          var label = 0.0
          var rowFeatures = 0L
          var rowSquares = 0.0
          while (from <= line.length && !malformed) {
            val tab = line.indexOf('\t', from)
            val to = if (tab < 0) line.length else tab
            if (to == from) {
              malformed = true
            } else {
              try {
                val value = line.substring(from, to).toDouble
                if (!java.lang.Double.isFinite(value)) malformed = true
                else if (field == 0) label = value
                else {
                  rowFeatures += 1
                  rowSquares += value * value
                }
              } catch {
                case _: NumberFormatException => malformed = true
              }
            }
            field += 1
            if (tab < 0) from = line.length + 1 else from = tab + 1
          }
          if (field != 2001 || rowFeatures != 2000 || !(label == 1.0 || label == -1.0 || label == 0.0)) malformed = true
          if (malformed) invalid += 1
          else {
            rows += 1
            if (label == 1.0) positive += 1 else negative += 1
            features += rowFeatures
            sumSquares += rowSquares
            maxIndex = math.max(maxIndex, 2000)
          }
        } else {
          val first = line.indexOf(' ')
          if (first <= 0) {
            invalid += 1
          } else {
          val label = try Some(line.substring(0, first).toDouble)
                      catch { case _: NumberFormatException => None }
          label match {
            case None => invalid += 1
            case Some(value) if value == 1.0 || value == -1.0 || value == 0.0 =>
              rows += 1
              if (value == 1.0) positive += 1 else negative += 1
              var from = first + 1
              var previousIndex = 0
              var rowFeatures = 0
              var malformed = false
              while (from < line.length && !malformed) {
                val nextSpace = line.indexOf(' ', from)
                val to = if (nextSpace < 0) line.length else nextSpace
                if (to > from) {
                  val colon = line.indexOf(':', from)
                  if (colon < from || colon >= to) {
                    malformed = true
                  } else {
                    try {
                      val index = line.substring(from, colon).toInt
                      val featureValue = line.substring(colon + 1, to).toDouble
                      if (index <= previousIndex || index > 2000 || !java.lang.Double.isFinite(featureValue)) {
                        malformed = true
                      } else {
                        previousIndex = index
                        maxIndex = math.max(maxIndex, index)
                        rowFeatures += 1
                        features += 1
                        sumSquares += featureValue * featureValue
                      }
                    } catch {
                      case _: NumberFormatException => malformed = true
                    }
                  }
                }
                from = to + 1
              }
              if (malformed || rowFeatures != 2000) invalid += 1
            case Some(_) => invalid += 1
          }
          }
        }
      }
    }
    if (holdSeconds > 0) Thread.sleep(holdSeconds.toLong * 1000L)
    PartitionSummary(host, partition, rows, positive, negative, features, invalid, sumSquares, maxIndex)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1 || args.length > 7 || !args(0).startsWith("file:///")) usage()
    val input = args(0)
    val partitions = if (args.length > 1) args(1).toInt else 8
    val holdSeconds = if (args.length > 2) args(2).toInt else 0
    val requiredHosts = if (args.length > 3) args(3).toInt else 2
    val expectedRows = if (args.length > 4) args(4).toLong else 0L
    val repartition = args.length > 5 && args(5).toBoolean
    val format = if (args.length > 6) args(6) else "libsvm"
    if (partitions < 2 || holdSeconds < 0 || requiredHosts < 1 || expectedRows < 0 ||
        !(format == "libsvm" || format == "tsv")) usage()

    val started = Instant.now().toString
    val start = System.nanoTime()
    val conf = new SparkConf().setAppName("Epsilon distributed data audit")
    val sc = new SparkContext(conf)
    var failed = false
    try {
      sc.setLogLevel("WARN")
      val rawSource = sc.textFile(input, partitions)
      val source = if (repartition) rawSource.repartition(partitions) else rawSource
      val results = source.mapPartitionsWithIndex(
        (index, lines) => Iterator.single(scanPartition(index, lines, holdSeconds, format)))
        .collect()
        .toSeq

      val rows = results.map(_.rows).sum
      val positive = results.map(_.positive).sum
      val negative = results.map(_.negative).sum
      val features = results.map(_.features).sum
      val invalid = results.map(_.invalid).sum
      val sumSquares = results.map(_.sumSquares).sum
      val maxIndex = results.map(_.maxIndex).foldLeft(0)(math.max)
      val hosts = results.map(_.host).distinct.sorted
      results.sortBy(_.partition).foreach { r =>
        println(f"PART host=${r.host} partition=${r.partition}%d rows=${r.rows}%d features=${r.features}%d invalid=${r.invalid}%d")
      }
      val elapsed = (System.nanoTime() - start).toDouble / 1e9
      println(f"RESULT app_id=${sc.applicationId} started=$started elapsed_s=$elapsed%.2f rows=$rows%d positive=$positive%d negative=$negative%d features=$features%d invalid=$invalid%d max_index=$maxIndex%d sum_squares=$sumSquares%.6f hosts=${hosts.mkString(",")} partitions=${results.size}%d")
      failed = invalid > 0 || rows == 0 || hosts.size < requiredHosts ||
          (expectedRows > 0 && rows != expectedRows)
      if (failed) {
        System.err.println(
          s"CHECK FAILED: invalid=$invalid rows=$rows expectedRows=$expectedRows hosts=${hosts.size} requiredHosts=$requiredHosts")
      }
    } finally {
      sc.stop()
    }
    if (failed) System.exit(3)
  }
}
