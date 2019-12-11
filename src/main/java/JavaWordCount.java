import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/*Java版本的WordCount*/
public class JavaWordCount {
    public static void main(String[] args) {
        //创建SparkContext对象，需系统配置：创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");//设置应用名称和设置运行模式
        //创建SparkContext对象
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        //对数据进行处理
        JavaRDD<String> line = javaSparkContext.textFile("dir/SparkCore_day1/file.txt");
        //切分数据 第一个泛型数据类型是String，是字符串输入值，第二个参数是数据类型的输出值
        JavaRDD<String> word = line.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> splite = Arrays.asList(s.split(" "));
                return splite.iterator();
            }
        });
        //对单词进行计数，形成元组
        //第一个泛型是单词的数据类型，第二个和第三个泛型是返回元组的数据类型
        JavaPairRDD<String, Integer> tuples = word.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //进行聚合
        JavaPairRDD<String, Integer> sum = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //相同key为一组进行计算  (hello,List(1,1,1,1,1,1))
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        /*排序取TopN*/
        //Java中没有提供SortBy算子，排序时需调用sortByKey，根据Key进行排序，因为是根据key排序，所以要交换元组
        JavaPairRDD<Integer, String> swaped = sum.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap(); //交换对偶元组的位置，可在Scala中通用
            }
        });
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);//false是降序
        //再次交换回来
        JavaPairRDD<String, Integer> res= sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        //TopN
        List<Tuple2<String, Integer>> top1 = res.take(1);
        System.out.println(top1);
        //如果需要，也可以保存到文件
        //res.saveAsTextFile("dir/SparkCore_day1/");
        //关闭对象
        javaSparkContext.stop();
    }
}
