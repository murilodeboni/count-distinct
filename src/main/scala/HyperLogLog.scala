import HyperLogLogIntermediateUtils._
import com.twitter.algebird.HyperLogLog.{fromBytes, int2Bytes, toBytes}
import com.twitter.algebird._

import java.io.File
import java.time.LocalDate

trait DisplayApproximate {
  def algName: String
  def aprox: Approximate[Long]
  def show: Unit = {
    println(f"$algName \n" +
      f"Min: ${aprox.min}, Max: ${aprox.max}, Estimate: ${aprox.estimate}, Prob: ${aprox.probWithinBounds}")
  }
}

case class BruteForce(dataList: Map[LocalDate, Vector[Int]]) extends DisplayApproximate {
  override def algName: String = "Brute Force"

  override def aprox: Approximate[Long] = {
    val distinct = dataList.values.reduce(_ ++ _).distinct.length.toLong
    Approximate(distinct, distinct, distinct, 1)
  }
}

case class HyperLogLogMonoidTest(bits: Int = 10, dateList: List[LocalDate]) extends DisplayApproximate {
  val hllMonoid = new HyperLogLogMonoid(bits = bits)
  override def algName: String = "HyperLogLogMonoid"

  override def aprox: Approximate[Long] = {
    val dateMap: List[HLL] = dateList.map(date => if (fileExists(date)) {
      fromBytes(readFileToArray(date))
    } else {
      getHLL(ReadFile(), date, hllMonoid)
    })

    // TODO: how to store intermediate steps?
    val combinedHLL = dateMap.reduce(_ + _)
    hllMonoid.sizeOf(combinedHLL)
  }

}

object HyperLogLog extends App {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def dateList(startDate: LocalDate, days: Int): List[LocalDate] = {
    if (days == 1) List(startDate.plusDays(1))
    else startDate.plusDays(1) :: dateList(startDate.plusDays(1), days - 1)
  }

  val dates = dateList(LocalDate.of(2019, 1, 1), 10)

  time { HyperLogLogMonoidTest(14, dates).show }
//  time { BruteForce(data).show }
}

object HyperLogLogIntermediateUtils {

  val PATH_TO_INTERMEDIATE_FILES = "./data/intermediate/"

  def saveHLLAsIntermediate(hll: HLL, date: LocalDate): Unit = {
    val byteArray = toBytes(hll)
    saveArrayInFolder(date, byteArray)
  }

  def getHLL(data: Vector[Int], date: LocalDate, hllMonoid: HyperLogLogMonoid): HLL = {
    val hllsVector: Vector[HLL] = data.map(hllMonoid.create(_))
    val hll = hllMonoid.sum(hllsVector)

    saveHLLAsIntermediate(hll, date)

    hll
  }
  def printToFile(f: File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
  def saveArrayInFolder(date: LocalDate, byteArray: Array[Byte]): Unit = {
    //save byteArray to disk in folder with name date
    printToFile(new File(f"$PATH_TO_INTERMEDIATE_FILES$date.txt")) { p =>
      byteArray.foreach(p.println)
    }
  }

  def fileExists(date: LocalDate): Boolean = new File(f"$PATH_TO_INTERMEDIATE_FILES$date.txt").exists()
  def readFileToArray(date: LocalDate): Array[Byte] = {
    //read file from disk and return array
    val source = scala.io.Source.fromFile(f"$PATH_TO_INTERMEDIATE_FILES$date.txt")
    val byteArray = source.getLines.map(_.toByte).toArray
    source.close()
    byteArray
  }
}