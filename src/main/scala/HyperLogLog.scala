import com.twitter.algebird.HyperLogLog.int2Bytes

import com.twitter.algebird._

trait DisplayApproximate {
  def algName: String
  def aprox: Approximate[Long]
  def show: Unit = {
    println(f"$algName \n" +
      f"Min: ${aprox.min}, Max: ${aprox.max}, Estimate: ${aprox.estimate}, Prob: ${aprox.probWithinBounds}")
  }
}

case class BruteForce(dataList: Vector[Vector[Int]]) extends DisplayApproximate {
  override def algName: String = "Brute Force"

  override def aprox: Approximate[Long] = {
    val distinct = dataList.reduce(_ ++ _).distinct.length.toLong
    Approximate(distinct, distinct, distinct, 1)
  }
}

case class HyperLogLogMonoidTest(bits: Int = 10, dataList: Vector[Vector[Int]]) extends DisplayApproximate {
  override def algName: String = "HyperLogLogMonoid"
  override def aprox: Approximate[Long] = {
    val hllMonoid = new HyperLogLogMonoid(bits = bits)
    val hllsList = dataList.map (
      data => data.map(hllMonoid.create(_))
    )
    val HLLs = hllsList.map(hllMonoid.sum(_))

    // TODO: how to store intermediate steps?
    val combinedHLL = HLLs.reduce(_ + _)
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

  val sample = ReadFile()
  val data = Vector(sample.users, sample.users2, sample.users3)

  time { HyperLogLogMonoidTest(14, data).show }
  time { BruteForce(data).show }
}