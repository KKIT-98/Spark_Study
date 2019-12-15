package SparkCore_05

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*需求:根据用户访问IP地址来统计所属区域并统计访问量*/
object IpCompute {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IpCompute").setMaster("local")
    val sc = new SparkContext(conf)
    //获取IP地址基础信息
    val ip: RDD[String] = sc.textFile("dir/SparkCore_day5/ip.txt")
    //1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    //对ip文件信息拆分，取出需要的（16777472，16778239）
    val splited: RDD[(String, String, String)] = ip.map(x => {
      //数据是以 | 形式分隔的
      val arr: Array[String] = x.split("\\|")
      //获取起始IP地址转换值和结束IP地址转换值以及省份
      (arr(2), arr(3), arr(6))
    })
    //如果定义的变量值经常被task任务所获取,会发生多次网络磁盘IO,就可以将这个变量存到广播变量
    //广播变量是不能广播RDD,所以此时还需要获取RDD中值,然后存到广播变量
    //转换完毕之后当前RDD就会变成Scala中数组,成为本地变量
    val localipinfo: Array[(String, String, String)] = splited.collect()
    //封装为广播变量
    val broadcastIpInfo: Broadcast[Array[(String, String, String)]] = sc.broadcast(localipinfo)
    //读取log文件中数据获取用户ip地址
    val http: RDD[String] = sc.textFile("dir/SparkCore_day5/http.log")
    val shenfentup: RDD[(String, Int)] = http.map(x => {
      val arr: Array[String] = x.split("\\|")
      //获取用户的IP地址,当前地址是123.197.46.211,需要将IP地址进行转换
      val iplong: Long = ipToLong(arr(1))
      //需通过转换后IP地址在已经封装到广播变量中数据进行查找
      //获取广播变量
      val getbroadvalue: Array[(String, String, String)] = broadcastIpInfo.value
      //二分查找，数组查找一种线性查找(从前到后一个一个比较)
      val index: Int = binerSeach(getbroadvalue, iplong)
      //ps:在获取具体数组中的值时,建议进行一次判断
      if(index > 0){ val shengfen: String = getbroadvalue(index)._3
        (shengfen, 1)
      }else{
        ("其他", 1)
      }
    })
    //20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/ EmbeddedWB- 14.59  from: http://bsalsa.com/ )|http://show.51.com/main.php|
    val res: RDD[(String, Int)] = shenfentup.reduceByKey(_ + _)
    // //将当前数据写入到数据库,必须使用JDBC驱动
    res.foreachPartition(DatatoMySql)
    res.foreach(println)
    sc.stop()
  }
  //对ip地址进行转换
  def ipToLong(ip:String) ={
    //将ip地址拆分
    val ips: Array[String] = ip.split("[.]")
    var ipNum = 0L
    //获取每个ip值  until 包含前面的不包含最后的
    for (i <- 0 until ips.length){
      //需要使用按位或 和向左位移8位
      //ps:按位或 只要对应两个二进制有任何一位为1时,结果就是1
      ipNum = ips(i).toLong | ipNum << 8L
    }
    //结果就是ip.txt文件中的其实IP转换值 16777472和结束IP转换值16778239
    ipNum
  }
  //通过二分查找获取对应ip地址索引
  //arr：要查找的数据
  //ip ：要查找的ip
  //找到ip地址返回下标，没有找到返回-1
  def binerSeach(arr:Array[(String,String,String)],ip:Long): Int ={
    //定义开始值和结束值
    var start = 0
    var end = arr.length - 1
    //开始值 > 结束值的时候 证明数据无法获取
    while (start <= end){
      //中间值
      val mid: Int = (start + end) / 2
      //对数组数据进行比较
      if((ip >= arr(mid)._1.toLong) && (ip <= arr(mid)._2.toLong)){
        return mid
      }
      if(ip < arr(mid)._1.toLong){
        //end向左移动
        end = mid - 1
      }else{
        start = mid + 1
      }
    }
    return -1 //没找到
  }
  //将结果发送到数据库中
   val DatatoMySql =(iter:Iterator[(String,Int)])=>{
     //创建连接对象和预编译语句
     var connect:Connection = null
     var ps:PreparedStatement = null
     //sql语句
     val sql:String = "insert into location_info(location,counts,access_date)values(?,?,?)"
     //jdbc连接驱动
     val jdbcurl:String = "jdbc:mysql://localhost:3306/sparkcore?useUnicode=true&characterEncoding=utf8"
     val user: String = "root"
     val password: String = "000000"
     //Scala对异常处理
     try {
       //连接
      connect =  DriverManager.getConnection(jdbcurl,user,password)
      //向语句添加数据
       iter.foreach(x =>{
         //加载预编译语句
         ps = connect.prepareStatement(sql)
         //?占位符进行赋值 ,序号从1开始逐渐递增
         ps.setString(1,x._1)
         ps.setInt(2,x._2)
         ps.setDate(3,new Date(System.currentTimeMillis()))
         ps.executeLargeUpdate()
       })
     }catch {
       case e:Exception => println(e.printStackTrace())
     }finally {
       if(ps != null){
         ps.close()
       }
       if(connect != null){
         connect.close()
       }
     }
   }
}
