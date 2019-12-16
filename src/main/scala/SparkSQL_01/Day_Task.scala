package SparkSQL_01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
部门表:
deptno 部门编号 dname - 部门名字 loc - 地址
员工表:
empno 员工编号
ename 员工姓名
job 岗位
mgr 直接领导编号
hiredate 入职日期
sal  薪水
comm  提成
deptno 部门编号
* */
/*列出至少有一个员工的所有部门。

列出薪金比"刘一"多的所有员工。
列出所有员工的姓名及其直接上级的姓名。
列出受雇日期早于其直接上级的所有员工。
列出部门名称和这些部门的员工信息，同时列出那些没有员工的部门。
列出所有job为“职员”的姓名及其部门名称。
列出最低薪金大于1500的各种工作。
列出在部门 "销售部" 工作的员工的姓名，假定不知道销售部的部门编号。
列出薪金高于公司平均薪金的所有员工。
列出在每个部门工作的员工数量、平均工资。
列出所有员工的姓名、部门名称和工资。
查出emp表中薪水在3000以上（包括3000）的所有员工的员工号、姓名、薪水。
查询出所有薪水在'陈二'之上的所有人员信息。
查出emp表中所有部门的最高薪水和最低薪水，部门编号为10的部门不显示。
删除10号部门薪水最高的员工。*/
object Day_Task {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Day_Task").setMaster("local")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //加载数据
    //员工数据
    val empno: RDD[String] = sc.textFile("dir/SparkSQL/Day_Task/empno.txt")
    //部门数据
    val deptno: RDD[String] = sc.textFile("dir/SparkSQL/Day_Task/deptno.txt")
    val empnotup: RDD[Empno] = empno.map(a => {
      val x: Array[String] = a.split(",")
      Empno(x(0).toInt, x(1), x(2), x(3), x(4), x(5).toInt, x(6),x(7).toInt)
    })
    val deptnotup: RDD[Deptno] = deptno.map(b => {
      val y: Array[String] = b.split(",")
      Deptno(y(0).toInt, y(1), y(2))
    })
    //RDD转换为DataFrame
    import spark.implicits._
    val empnoframe: DataFrame = empnotup.toDF()
    val deptnofarme: DataFrame = deptnotup.toDF()
    //创建临时表
    empnoframe.createOrReplaceTempView("empno")
    deptnofarme.createOrReplaceTempView("deptno")
    //列出至少有一个员工的所有部门。
    //spark.sql("select distinct b.deptno,b.dname from empno as a join deptno as b on a.deptno = b.deptno").show()
    //列出薪金比"刘一"多的所有员工。
    //spark.sql("select * from empno where sal > (select sal from empno where ename = \"刘一\")").show()
    //列出所有员工的姓名及其直接上级的姓名。
    //spark.sql("select a.ename,b.ename from empno a join empno b on a.empno = b.empno where a.empno = b.hiredate").show()
    //列出受雇日期早于其直接上级的所有员工。

    //列出部门名称和这些部门的员工信息，同时列出那些没有员工的部门。
    //spark.sql("select b.dname,a.* from empno a right join  deptno b on a.deptno = b.deptno").show()
    //列出所有job为“职员”的姓名及其部门名称。
    //spark.sql("select a.ename,b.dname from empno a join  deptno b on a.deptno = b.deptno where a.job = \"职员\"").show()
    //列出最低薪金大于1500的各种工作。
    //spark.sql("select job from empno where sal > 1500").show()
    //列出在部门 "销售部" 工作的员工的姓名，假定不知道销售部的部门编号。
    //spark.sql("select a.ename from empno a join deptno b on a.deptno = b.deptno where b.dname=\"销售部\"").show()
    //列出薪金高于公司平均薪金的所有员工。
    //spark.sql("select * from empno where sal > (select avg(sal) from empno)").show()
    //列出在每个部门工作的员工数量、平均工资。
    //spark.sql("select deptno,avg(sal) from empno group by deptno").show()
    //列出所有员工的姓名、部门名称和工资。
    spark.sql("select a.ename as ename,b.dname dname,a.sal sal from empno a join deptno b on a.deptno = b.deptno").show()
    //查出emp表中薪水在3000以上（包括3000）的所有员工的员工号、姓名、薪水。
    spark.sql("select empno,ename,sal from empno where sal >= 3000").show()
    //查询出所有薪水在'陈二'之上的所有人员信息。
    //spark.sql("").show()
    //查出emp表中所有部门的最高薪水和最低薪水，部门编号为10的部门不显示。
    spark.sql("select b.dname,max(a.sal),min(a.sal) from empno a join deptno b on a.deptno = b.deptno group by b.dname and a.deptno != 10 ").show()
    //删除10号部门薪水最高的员工。
   // spark.sql("").show()
    sc.stop()
    spark.stop()
  }
}
case class Empno(empno:Int,ename:String,job:String,mgr:String,hiredate:String,sal:Int,comm:String,deptno:Int)
case class Deptno(deptno:Int,dname:String,loc:String)
