package SparkStreaming_02

object ScalaWindow {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    //想显示出 1,2  2,3   3,4  4,5  5,6  6,7  7,8
    //scala提供了一个方法sliding   第一个参数：窗口大小   第二个人参数：步长
    val iterator: Iterator[List[Int]] = list.sliding(2,2)
    for (i <- iterator){
      println(i.mkString(","))
    }
  }
}
