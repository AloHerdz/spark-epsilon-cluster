import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

/** Deterministic LIBSVM smoke data generator with the Epsilon feature count. */
object MakeSample {
  def main(args: Array[String]): Unit = {
    val rows = if (args.length > 0) args(0).toInt else 512
    if (rows < 1 || rows > 100000) {
      System.err.println("rows must be between 1 and 100000")
      System.exit(2)
    }
    val output: Path = if (args.length > 1) Paths.get(args(1)) else Paths.get("data/sample.libsvm")
    Files.createDirectories(output.toAbsolutePath.getParent)
    val writer = new PrintWriter(Files.newBufferedWriter(output, StandardCharsets.US_ASCII))
    try {
      var row = 0
      while (row < rows) {
        val label = if (row % 2 == 0) "+1" else "-1"
        val line = new StringBuilder(30000)
        line.append(label)
        var feature = 1
        while (feature <= 2000) {
          val raw = ((row.toLong * 17 + feature.toLong * 31) % 2001 - 1000).toDouble / 1000.0
          line.append(' ').append(feature).append(':')
            .append(String.format(java.util.Locale.ROOT, "%.3f", Double.box(raw)))
          feature += 1
        }
        writer.println(line.toString)
        row += 1
      }
    } finally writer.close()
    println(s"${output.toAbsolutePath} rows=$rows bytes=${Files.size(output)}")
  }
}
