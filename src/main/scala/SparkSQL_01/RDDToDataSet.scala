package SparkSQL_01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/*通过反射获取Schema  RDD 到 DataSet*/
object RDDToDataSet {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    //获取数据
    val line: RDD[String] = sc.textFile("dir/SparkSQL/dataSource/people.txt")
    //1.将RDD转换为DataSet,需要先提供样例类
    //2.通过处理将数据写入到样例类中,形成RDD

    val people: RDD[Peoples] = line.map(x => {
      val arrs: Array[String] = x.split(",")
      Peoples(arrs(0), arrs(1).trim.toInt)
    })
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //3.通过RDD调用.toDS方法触发DataSet(必须进行隐式转换导入)
    import session.implicits._
    val value: Dataset[Peoples] = people.toDS()
    value.show()
    //DataSet转换为RDD 直接调用rdd方法
    val rdd: RDD[Peoples] = value.rdd
    rdd.foreach(p => println(p.Name))
    //DataSet转换为DataFrame
    //只需要DataSet对象.toDF方法即可,因为DataSet是知道数据类型的 即样例类
    //所以转换为DataFrame时 frame对象自动获取样例类中封装的属性和属性类型
    value.toDF().show()
    sc.stop()
  }
}
case class Peoples(Name:String,Age:Int)
