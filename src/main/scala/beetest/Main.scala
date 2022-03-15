package beetest


object Main {
  def main(args: Array[String]): Unit = {
    val jobs: Array[() => Unit] = Array(Task1.job, Task2.job)
    args(0).toIntOption match {
      case Some(num) if num >= 1 && num <= 2 => jobs(num-1)()
      case None => println("Input job number has illegal format exception")
      case _ => println("Input job number isn't in correct interval")
    }
  }
}