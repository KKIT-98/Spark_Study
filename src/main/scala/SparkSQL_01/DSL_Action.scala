package SparkSQL_01
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object DSL_Action {
  def main(args: Array[String]): Unit = {
    //以下方法触发的返回值都是DataFrame,可以连续调用
    val session: SparkSession = SparkSession.builder().appName("DSL_Action").master("local").getOrCreate()
    //读取数据：员工
    val empyee: DataFrame = session.read.json("dir/SparkSQL/dataSource/employee.json")
    //读取数据：部门
    val department: DataFrame = session.read.json("dir/SparkSQL/dataSource/department.json")
    //where条件:等同于SQL语言中的where关键字,传入要筛选的表达式,可以使用and或or,返回DataFrame
    empyee.where("depId=3").show()
    empyee.where("depId=3 and salary > 18000").show()
    //filter过滤:传入筛选条件,得到DataFrame返回值 <==> where
    empyee.filter("depId=3 and salary > 18000").show()
    //select:根据传入String类型的字段名,获取指定字段的值,以DataFrame返回值
    empyee.select("name","age").show(3)
    empyee.select("name","age").where("age > 30").show()
    //select方法有一个重载,参数不是String类型参数,而是Column类型参数,Column直理解字面意思即可 列  它是DataFrame中所封装一个数据类(看做了一个类型)
    val column: Column = empyee("age")
    empyee.select(empyee("name"),empyee("age"),column+1).show()
    //selectExpr:增强select ,可以传入函数(UDF),别名,字段得到是一个DataFrame对象
    empyee.selectExpr("name","depId as DEPID","round(salary,1)").show()
    session.stop()

  }
}
