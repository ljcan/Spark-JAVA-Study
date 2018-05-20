package cn.just.spark.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * transformation操作小案例实验
 * @author shinelon
 *
 */
public class TransformationOperation {
	public static void main(String[] args) {
//		map();
//		filter();
//		 flatMap();
//		groupByKey();
//		reduceByKey();
//		sortByKey();
//		join();
		cogroup();
	}
	
	/**
	 * map操作,将集合中的元素乘以2打印
	 */
	public static void map(){
		SparkConf conf=new SparkConf()
				.setAppName("map")
				.setMaster("local");
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Integer> list=Arrays.asList(1,2,3,4,5);
		
		//并行化集合，初始化RDD
		JavaRDD<Integer> numRDD=context.parallelize(list);
		//map操作传入的参数是Function对象
		//创建Function对象必须要自己指定第二个泛型参数，并且call函数返回值与第二个泛型参数类型一致
		JavaRDD<Integer> mapRDD=numRDD.map(new Function<Integer,Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		
		mapRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		context.close();
	}
	/**
	 * filter操作，过滤集合中偶数
	 */
	public static void filter() {
		SparkConf conf=new SparkConf()
				.setAppName("map")
				.setMaster("local");
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Integer> list=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		
		//并行化集合，初始化RDD
		JavaRDD<Integer> numRDD=context.parallelize(list);
		//使用filter操作过滤，传入的参数是Function对象
		//Function对象传入的第二个泛型参数是boolean,返回true的对象将被保留
		JavaRDD<Integer> filterRDD=numRDD.filter(new Function<Integer, Boolean>() {
			@Override
			public Boolean call(Integer v1) throws Exception {
				return v1%2==0;
			}
		});
		filterRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		context.close();
	}
	/**
	 * flatMap算子案例：将行文本转换为多个单词
	 */
	public static void flatMap() {
		SparkConf conf=new SparkConf()
				.setAppName("flatMap")
				.setMaster("local");
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<String> lines=Arrays.asList("Hello world","Hello Spark","Be Happy");
		//并行化集合，创建初始化RDD
		JavaRDD<String> line=context.parallelize(lines);
		//第二个泛型参数为输出返回结果
		JavaRDD<String> words=line.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});
		
		//遍历打印集合
		words.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		context.close();
	}
	/**
	 * GroupByKey算子案例分析:对班级成绩进行分组
	 */
	public static void groupByKey() {
		SparkConf conf=new SparkConf()
				.setAppName("groupByKey")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Tuple2<String, Integer>> listScores=Arrays.asList(
				new Tuple2<String, Integer>("class1", 85),
				new Tuple2<String, Integer>("class2", 80),
				new Tuple2<String, Integer>("class2", 90),
				new Tuple2<String, Integer>("class1", 84)
				);
		//并行化集合，创建初始化RDD
		JavaPairRDD<String, Integer> socres=context.parallelizePairs(listScores);
		//按照key进行分组，因为一个key可能有多个值，因此会聚合为Iterable类型参数
		JavaPairRDD<String, Iterable<Integer>> groupScores=socres.groupByKey();
		//打印分组后的结果
		groupScores.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println("class: "+t._1);
				Iterator<Integer> it=t._2.iterator();
				while(it.hasNext()) {
					System.out.println(it.next());
				}
				System.out.println("====================");
				
			}
		});
		context.close();
	}
	/**
	 * reduceByKey算子案例：统计一个班级的总分数
	 */
	public static void reduceByKey() {
		SparkConf conf=new SparkConf()
				.setAppName("reduceByKey")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Tuple2<String, Integer>> listScores=Arrays.asList(
				new Tuple2<String, Integer>("class1", 85),
				new Tuple2<String, Integer>("class2", 80),
				new Tuple2<String, Integer>("class2", 90),
				new Tuple2<String, Integer>("class1", 84)
				);
		//并行化集合，创建初始化RDD
		JavaPairRDD<String, Integer> socres=context.parallelizePairs(listScores);
		//对成绩进行reduce操作，传入的参数是Function2类型匿名内部类，总共三个泛型参数
		//第一个和第二个泛型参数是按照key进行分组后的value，对应着call方法的两个参数类型
		//第三个泛型参数是进行reduce操作后的返回值类型
		JavaPairRDD<String, Integer> totalScores=socres.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		//打印总成绩
		totalScores.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+" :"+t._2);
			}
		});
		
		
		context.close();
	}
	/**
	 * sortByKey算子操作：按照成绩进行排名
	 */
	public static void sortByKey() {
		SparkConf conf=new SparkConf()
				.setAppName("reduceByKey")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Tuple2<Integer, String>> listScores=Arrays.asList(
				new Tuple2<Integer,String>(60, "tom"),
				new Tuple2<Integer,String>(80, "marry"),
				new Tuple2<Integer,String>(95, "siily"),
				new Tuple2<Integer,String>(75, "lili")
				);
		
		JavaPairRDD<Integer, String> scoreRDD=context.parallelizePairs(listScores);
		
		//默认升序排列，传入false表示按照降序排列
		JavaPairRDD<Integer, String> sortScores=scoreRDD.sortByKey(false);
		
		sortScores.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1+" :"+t._2);
			}
		});
		context.close();
	}
	/**
	 * join算子的案例：对两个RDD进行join操作
	 */
	public static void join() {
		SparkConf conf=new SparkConf()
				.setAppName("reduceByKey")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Tuple2<Integer,String>> students=Arrays.asList(
				new Tuple2<Integer,String>(1, "shinelon"),
				new Tuple2<Integer,String>(2,"tom"),
				new Tuple2<Integer,String>(3, "marry")
				);
		List<Tuple2<Integer, Integer>> scores=Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer,Integer>(2, 80),
				new Tuple2<Integer,Integer>(3, 85)
				);
		//并行化上面两个集合
		JavaPairRDD<Integer, String> studentRDD=context.parallelizePairs(students);
		JavaPairRDD<Integer, Integer> scoreRDD=context.parallelizePairs(scores);
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores=studentRDD.join(scoreRDD);
		
		studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
				System.out.println("student ID:"+t._1);
				System.out.println("student Name:"+t._2._1);
				System.out.println("student Scores:"+t._2._2);
				System.out.println("================================");
			}
		});
		context.close();
	}
	/**
	 * cogroup算子的案例：对两个RDD进行cogroup操作
	 */
	public static void cogroup() {
		SparkConf conf=new SparkConf()
				.setAppName("reduceByKey")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Tuple2<Integer,String>> students=Arrays.asList(
				new Tuple2<Integer,String>(1, "shinelon"),
				new Tuple2<Integer,String>(2,"tom"),
				new Tuple2<Integer,String>(3, "marry")
				);
		List<Tuple2<Integer, Integer>> scores=Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer,Integer>(2, 80),
				new Tuple2<Integer,Integer>(3, 85),
				new Tuple2<Integer,Integer>(2, 74),
				new Tuple2<Integer,Integer>(3, 85),
				new Tuple2<Integer,Integer>(1, 95)
				);
		//并行化上面两个集合
		JavaPairRDD<Integer, String> studentRDD=context.parallelizePairs(students);
		JavaPairRDD<Integer, Integer> scoreRDD=context.parallelizePairs(scores);
		
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores=studentRDD.cogroup(scoreRDD);
		
		studentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
				System.out.println("student ID:"+t._1);
				System.out.println("student Name:"+t._2._1);
				System.out.println("student Scores:"+t._2._2);
				System.out.println("================================");
			}
		});
		context.close();
	}
	
}
