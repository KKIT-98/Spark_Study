package SparkSQL_01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*通过反射获取Schema:过反射的形式来获取当前数据类型(可以用一个样例类来封数据)*/
object RDDToDataFrame2 {
  def main(args: Array[String]): Unit = {
    //schema信息 RDD 到 DataFrame
    val conf: SparkConf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    //获取数据
    val line: RDD[String] = sc.textFile("dir/SparkSQL/dataSource/people.txt")
    //通过反射的形式来获取当前数据类型(可以用一个样例类来封数据)
    val people: RDD[Peoples] = line.map(x => {
      val arrs: Array[String] = x.split(",")
      Peoples(arrs(0), arrs(1).trim.toInt)
    })
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //需要提供隐式转换导入
    import spark.implicits._
    val frame: DataFrame = people.toDF()
    frame.show()
    //DataFrame转为DataSet
    //因为DataSet获取数据时需要知道数据类型,所以DataFrame若转换为DataSet建议使用样例类模式
    //ps:也就是 RDD 到DataFrame 第二种模式
    //frame.as[frame中分装的数据类型]
    val dataset: Dataset[Peoples] = frame.as[Peoples]
    dataset.show()
    sc.stop()
  }
}
case class Peoples(Name:String,Age:Int)