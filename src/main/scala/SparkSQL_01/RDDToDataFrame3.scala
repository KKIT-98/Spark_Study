package SparkSQL_01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/*3.通过StructType直接指定Schema*/
object RDDToDataFrame3 {
  def main(args: Array[String]): Unit = {
    /* 通过StructType  从RDD 到  DataFrame*/
    val conf: SparkConf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    //获取数据
    val line: RDD[String] = sc.textFile("dir/SparkSQL/dataSource/people.txt")
    //将数据映射到Row对象中
    val rddRow: RDD[Row] = line.map(x => {
      val arrs: Array[String] = x.split(",")
      Row(arrs(0), arrs(1).trim.toInt)
    })
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //通过StructType指定Schema信息
    val structschema: StructType = StructType(
      List(
        //有多少数据就写多少,需要指定列名称和列的数据类型,true代表可以为空
        StructField("Name", StringType, true),
        StructField("Age", IntegerType, true)
      )
    )
    ///将schema信息和rowRDD进行整合创建DataFrame
    val frame: DataFrame = spark.createDataFrame(rddRow, structschema)
    frame.show()
    //DataFrame转换为RDD无论通过哪种方式获取DataFrame都可以使用一个方法
    //返回类型是一个RDD[Row],RDD最常用处理是元组, 将RDD中Row转换为元组继续数即可
    val rdd: RDD[Row] = frame.rdd
    //传入参数是下标
    val value: RDD[(String, Int)] = rdd.map(row => (row.getString(0), row.getInt(1)))
    val value1: RDD[(Any, Any)] = rdd.map(row => (row(0), row(1)))
    println(value.collect().toBuffer)
    println(value1.collect().toBuffer)
    sc.stop()
  }
}
