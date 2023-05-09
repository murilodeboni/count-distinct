import HyperLogLogIntermediateUtils.saveArrayInFolder
import com.twitter.algebird.HyperLogLog.{fromBytes, int2Bytes, toBytes}
import com.twitter.algebird._

import java.io.File
import java.time.LocalDate
import scala.collection.immutable

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

case class HyperLogLogMonoidTest(bits: Int = 10, dataList: Map[LocalDate, Vector[Int]]) extends DisplayApproximate {
  override def algName: String = "HyperLogLogMonoid"
  override def aprox: Approximate[Long] = {
    val hllMonoid = new HyperLogLogMonoid(bits = bits)
    val hllsList: Map[LocalDate, Vector[HLL]] = dataList.map {
      case (date, data) => date -> data.map(hllMonoid.create(_))
    }

    val HLLs = hllsList.map{case (date, hllVector) => (date, hllMonoid.sum(hllVector))}

    val intermediate = HLLs.map{case (date, hll) => (date, toBytes(hll))}

    intermediate.foreach{case (date, hll) => saveArrayInFolder(date, hll)}

    val fromIntermediateToHLL = intermediate.map(i => fromBytes(i._2))

    // TODO: how to store intermediate steps?
    val combinedHLL = fromIntermediateToHLL.reduce(_ + _)
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

  val date1: LocalDate = LocalDate.of(2019, 1, 1)
  val date2: LocalDate = LocalDate.of(2019, 1, 2)
  val date3: LocalDate = LocalDate.of(2019, 1, 2)

  val sample = ReadFile()

  val data = Map(date1 -> sample.users, date2 -> sample.users2, date3 -> sample.users3)

  time { HyperLogLogMonoidTest(14, data).show }
//  time { BruteForce(data).show }
}

object HyperLogLogIntermediateUtils {

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
    printToFile(new File(f"$date.txt")) { p =>
      byteArray.foreach(p.println)
    }
  }

  def fileExists(date: LocalDate): Boolean = new File(f"$date.txt").exists()
  def readFileToArray(date: LocalDate): Array[Byte] = {
    //read file from disk and return array
    val source = scala.io.Source.fromFile(f"$date.txt")
    val byteArray = source.getLines.map(_.toByte).toArray
    source.close()
    byteArray
  }
}