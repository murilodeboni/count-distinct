import com.twitter.algebird.HyperLogLog.int2Bytes

import com.twitter.algebird._

trait Aproximate {
  def algName: String
  def aprox: Approximate[Long]
  def show: Unit = {
    println(f"$algName \n" +
      f"Min: ${aprox.min}, Max: ${aprox.max}, Estimate: ${aprox.estimate}, Prob: ${aprox.probWithinBounds}")
  }
}

case class HyperLogLogMonoidTest(bits: Int = 10, data: Vector[Int]) extends Aproximate {
  override def algName: String = "HyperLogLogMonoid"
  override def aprox: Approximate[Long] = {
    val hllMonoid = new HyperLogLogMonoid(bits = bits)
    val hlls = data.map {
      hllMonoid.create(_)
    }
    val combinedHLL = hllMonoid.sum(hlls)
    hllMonoid.sizeOf(combinedHLL)
  }

}

case class HyperLogLogAggregatorTest(err: Double = 0.01, data: Vector[Int]) extends Aproximate {
  override def algName: String = "HyperLogLogAggregator"
  override def aprox: Approximate[Long] = {
    val agg = HyperLogLogAggregator.withErrorGeneric[Int](err)
    val combinedHLLAgg = agg(data)
    combinedHLLAgg.approximateSize
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

  val data = ReadFile().users

  time { HyperLogLogAggregatorTest(0.01, data).show }
  time { HyperLogLogMonoidTest(14, data).show }
}