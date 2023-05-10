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
}

object ReadFile {
  def apply(path: String = "./data/full.csv"): Vector[Int] = new ReadFile(path).users
}
