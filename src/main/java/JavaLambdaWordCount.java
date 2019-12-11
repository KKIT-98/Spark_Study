import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaLambdaWordCount {
    public static void main(String[] args) {
        //Java lambda表达式版本的WordCount  注:只支持jdk1.8及以上
        SparkConf conf = new SparkConf().setAppName("JavaLambdaWordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //数据处理
        JavaRDD<String> lines = sc.textFile("dir/SparkCore_day1/file.txt");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //生成元组
        JavaPairRDD<String, Integer> tup = words.mapToPair(word -> new Tuple2<>(word, 1));
        //聚合
        JavaPairRDD<String, Integer> sums = tup.reduceByKey((v1, v2) -> v1 + v2);
        //交换kv
        JavaPairRDD<Integer, String> swaped = sums.mapToPair(tuple -> tuple.swap());
        //降序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //交换
        JavaPairRDD<String, Integer> res = sorted.mapToPair(t -> t.swap());
        System.out.println(res.collect());
        // res.saveAsTextFile("./out");
        sc.stop();
    }
}
