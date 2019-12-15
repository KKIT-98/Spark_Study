package SparkCore_04
//自定义排序2:样例类实现排序(推荐方式)
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CustomSort2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val info: RDD[(String, Int, Int)] = sc.parallelize(List(("xiaohong", 30, 32), ("XiaoMin", 20, 23), ("LiHua", 50, 33), ("lisi", 80, 89)))
    //对存储在元组中的颜值进行排序，也就是名字后面的
    val value: RDD[(String, Int, Int)] = info.sortBy(_._2,false)
    println(value.collect().toBuffer)
    println("自定义排序2")
    val value1: RDD[(String, Int, Int)] = info.sortBy(t => Teacher1(t._2, t._3))
    println(value1.collect().toBuffer)
  }

}
//第二种排序方式(使用样例类)--> 样例类也可以使用类似于普通类一样的操作
case class Teacher1(face:Int,age:Int) extends Ordered[Teacher1]{
  override def compare(that: Teacher1): Int = {
    if(this.face == that.face){
      that.age - this.age
    }else{
      this.face - that.face
    }
  }
}