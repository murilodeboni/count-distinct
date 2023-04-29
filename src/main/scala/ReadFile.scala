import scala.io.BufferedSource

case class ReadFile(path: String = "./data/full.csv") {
  val bufferedSource: BufferedSource = io.Source.fromFile(path, enc = "ISO-8859-1")
  var users: Vector[Int] = Vector()
  for (line <- bufferedSource.getLines) {
    try {
      users = users :+ line.split(";").head.drop(1).dropRight(1).toInt
    } catch {
      case _: Throwable => //println("error in " + line)
    }
  }
  val users2: Vector[Int] = users.map(_+10).drop(10000)
  val users3: Vector[Int] = users.map(_+50).dropRight(50000)
  bufferedSource.close
}
