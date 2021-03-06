package cn.just.spark.core;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Function;
import scala.Tuple2;

/**
 * 本地编写测试Spark wordcount程序
 * @author shinelon
 *
 */
public class SortWordCountLocal {
	public static void main(String[] args) {
		long start=System.currentTimeMillis();
		//创建SparkConf对象
		//setMaster可以设置要连接的spark集群上的master节点所在的url，如果设置为local，则在本地运行
		SparkConf conf=new SparkConf()
				.setAppName("WordCountLocal")
				.setMaster("local");
		//创建JavaSparkContext对象
		JavaSparkContext sc=new JavaSparkContext(conf);
		//读取数据，创建第一个RDD数据集
		//如果RDD是读取的文件数据，那么RDD的每一个元素就是文件的一行数据
		JavaRDD<String> lines=sc.textFile("C:/Users/shinelon/Desktop/spark.txt");
		//如果处理的简单通常会创建一个匿名内部类，如果复杂则会单独创建一个类来处理
		//FlatMapFunction函数有两个泛型参数，第一个泛型参数是值输入，第二是输出
		JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
 		});
		//将每个单词映射为（word，1）这样的格式
		JavaPairRDD<String, Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		//计算每一个单词出现的次数，使用ReduceByKey算子
		JavaPairRDD<String,Integer> wordCounts=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		//将RDD反转，之前为（hello,2）的形式转换为(2，hello)
		JavaPairRDD<Integer, String> sortWordCount=wordCounts.mapToPair(
				new PairFunction<Tuple2<String,Integer>, Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						Integer key=t._2;
						String value=t._1;
						return new Tuple2<Integer, String>(key, value);
						}
		});
		//打印结果:按照key排序，即词频大小进行排序
		
		sortWordCount.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer,String>>() {

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1+" : "+t._2);
			}
		});
		
		sc.close();
		System.out.println("time: "+(System.currentTimeMillis()-start)+" ms");
	}
}
