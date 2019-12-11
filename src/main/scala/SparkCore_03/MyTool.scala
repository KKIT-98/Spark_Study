package SparkCore_03

import org.apache.spark.{SparkConf, SparkContext}

object MyTool {
    //创建SparkContext工具类
  def createSparkContext(AppName:String,Master:String) = {
    val conf: SparkConf = new SparkConf().setAppName(AppName).setMaster(Master)
    val sc = new SparkContext(conf)
    sc
  }
}
