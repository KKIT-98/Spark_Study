package SparkSQL_01
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

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
    // 提供个一个简化方式  col 代表了 Column  col需要触发 DataFrame对象
    empyee.select(empyee.col("name")).show()
    empyee.select(empyee("name")).show()
    val column: Column = empyee("age")
    empyee.select(empyee("name"),empyee("age"),column+1).show()
    //selectExpr:增强select ,可以传入函数(UDF),别名,字段得到是一个DataFrame对象
    empyee.selectExpr("name","depId as DEPID","round(salary,1)").show()
    //drop:去除(删除)指定字段,返回一个新的Dataframe对象 一次只能去除一个
    empyee.drop("gender").show()
    //limit:提交获取DataFrame对象前n行记录,返回的是一个DataFrame limit和 take和head不同点在于limit不是Action
    empyee.limit(3).show()
    //排序:order by 和 sort
    //指定字段进行排序, 默认是升序,添加"-"号表示降序, 只能对数据类型或日期类型进行排序 而且两则相同
    // 需要准守语法规则  orderBy返回类型是DataSet对象  sort返回类型对象是DataSet对象
    val v1: Dataset[Row] = empyee.orderBy(-empyee("salary"))
    val v2: Dataset[Row] = empyee.sort(empyee("salary"))
    //sortWithinPartitions:操作和sort是一样区别在与sortWithinPartitions方法返回的是Partiton排序好 返回的也是DataSet对象
    val vale: Dataset[Row] = empyee.repartition(1)
    vale.sortWithinPartitions("salary").show()
    //groupBy:分组,可以对指定这段进行分组操作,可以指定多个字段两种传递参数 一种是column对象 另外一种是String 类型列的名字
    //    //grouBy得到是分组之后的对象,所以遵循SQL的语法规则,可以添加聚合函数 ,max  min  mean(平均值) sum  count
    //    //不添加聚合函数 返回值时GroupDataSet  如果添加聚合函数 返回就是DataFrame
    val count: DataFrame = empyee.groupBy("depId").count
    count.show()
    ///多个维度的分组 cube 和 rollup
    //多个维度的分组 cube 和 rollup
    //cube和rollup是groupby的增强版本,相当于是多个groupby执行 cube和rollup是多维度到的数据分区聚合
    /*
      rollup 在指定表达式的每个层次级别创建分组集
      ps:  group by (A,B,C) wiht rollup(SQL语法)
          首先会对A,B,C三个字段进行group by 然后会对 A ,B 进行group by 然后在对 A 进行group by ,最后全表进行group by

      rollup不能排序,两个关键字互斥
      如果分组中列包含NUll值,那么rollup的结果可能不会准确,需要处理null值的列
+-----+------+------------------+
|depId|gender|       sum(salary)|
+-----+------+------------------+
|    3|female|           79000.0|
|    1|  male|35000.126000000004|
|    1|  null|35000.126000000004|
| null|  null|        211000.126|
|    3|  null|          115000.0|
|    2|  male|            8000.0|
|    3|  male|           36000.0|
|    2|  null|           61000.0|
|    2|female|           53000.0|
+-----+------+------------------+
等价于语句
   select  depId,gender,sum(salary) from employee   //表示对这2类进行分组聚合相当于是groupby本的聚集
   group by depId,gender
   union
   select depId,null,sum(salary) from employeed  //表示仅对(depId)一类进行分组聚合
   group by depId
      union
   select null,null,sum(salary) from employee  //全表聚合
     */
    empyee.rollup("depId","gender").sum("salary").show()


    //rollup 是cube的一种特殊情况,cube也是一对数据聚合的操作,但是在rollup的基础之上进行再次聚合
    //如果 rollup具有N个维度  cube需要2的N此房的分组维度
    //cube(A,B,C)   首先会对 A,B,C 进行分组,然后依次执行(A,B),(A,C),(A) (B,C),(B) ,(C),最后在做全表的groupby
    /*
 +-----+------+------------------+
|depId|gender|       sum(salary)|
+-----+------+------------------+
|    3|female|           79000.0|
|    1|  male|35000.126000000004|
|    1|  null|35000.126000000004|
| null|  null|        211000.126|
|    3|  null|          115000.0|
|    2|  male|            8000.0|
|    3|  male|           36000.0|
| null|female|          132000.0|
|    2|  null|           61000.0|
| null|  male|         79000.126|
|    2|female|           53000.0|
+-----+------+------------------+
cube的结果集是在 rollup的基础上在进行增加
select depId,gender.sum(salary) from employee
group by depId,gender with rollup
相当于执行
select  depId,gender,sum(salary) from employee   //表示对这2类进行分组聚合相当于是groupby本的聚集
     group by depId,gender
     union
     select depId,null,sum(salary) from employeed  //表示仅对(depId)一类进行分组聚合
     group by depId
        union
     select null,null,sum(salary) from employee  //全表聚合

select depId,gender.sum(salary) from employee
group by depId,gender with rollup
union
select null,gender.sum(salary) from employee
group by  gender


     */
    empyee.cube("depId","gender").sum("salary").show()

    //distinct:去重。返回当前DataSet中不重复Row记录
    empyee.distinct().show()
    //dropDuplicates:去重指定指定字段
    empyee.dropDuplicates(Seq("name")).show()
    //agg:聚合 当前聚合是DataFrame中特有的,该方法有多种调用,可以和groupby进行配合使用key是字段 value 是操作
    empyee.agg("age"->"max","salary"->"sum").show
    //union:对两个DataFrame进行合并
    empyee.union(empyee.limit(1)).show()
    //join  两个表聚合

    //1. using一个字段形式
    //下面这种join操作类似于 a join b using column的形式 ,需要两个DataFrame有相同一个列

    //读取数据(员工)
    val employeeDF_2:DataFrame = session.read.json("dir/SparkSQL/dataSource/employee.json")
    empyee.join(employeeDF_2,"depid").show()

    //2.可以使用using多个字段
    empyee.join(employeeDF_2,Seq("name","depId")).show()

    //3.指定join类型
    //需要在指定字段的前提下,可以使用参数指定join的类型
    /*
       inner  内连接
        outer full full_outer  全连
        left  left_outer 左连
        right right_outer 右连接

        left_semi   过滤出 join操作 DF1和DF2中共有的部分
        left_anti   过滤出 join操作  DF1和DF2中没有的部分
     */
    empyee.join(employeeDF_2,Seq("name","depId"),"inner").show()

    //不是using模式,改用column模式
    empyee.join(empyee,empyee("depId") === department("id")).show()
    //指定当前连接方式
    empyee.join(department,empyee("depId") === department("id"),"inner").show()

    //交集 计算两个DataFrame中相同记录
    empyee.intersect(employeeDF_2.limit(1)).show()

    //获取一个DataFrame中有另外一个DataFrame中没有记录
    empyee.except(employeeDF_2.limit(1)).show

    //修改字段字段名字
    //名字不存在就不做操作,否则就做字段名修改
    empyee.withColumnRenamed("depId","deprtId").show()

    //新增加一列字段(复制一列)
    //根据传入字段名字在DataFrame新增加一列 若类存在,则覆盖当前列
    empyee.withColumn("salary2",empyee("salary")).show()
    //行转列
    //需要对某个字段内存进行分割,然后形成多行,可以使用 explode方法  过时了,建议使用flatMap或select做
    empyee.explode("name","name_"){x:String => x.split(" ")}.show
    session.stop()

  }
}
