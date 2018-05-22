package cn.just.spark.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class ActionOperation {
	public static void main(String[] args) {
//		reduce();
//		collect();
//		count();
//		take();
//		saveAsTextFile();
		countByKey();
	}
	
	public static void reduce() {
		SparkConf conf=new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Integer> list=Arrays.asList(1,2,3,4,5);
		
		//并行化集合，创建初始化RDD
		JavaRDD<Integer> initRDD=context.parallelize(list);
		//将集合叠加
		Integer sum=initRDD.reduce(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		System.out.println(sum);
		context.close();
	}
	/**
	 * collect案例：将集合中的每一个元素乘以2
	 */
	public static void collect() {
		SparkConf conf=new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Integer> list=Arrays.asList(1,2,3,4,5);
		//并行化集合，创建初始化RDD
		JavaRDD<Integer> initRDD=context.parallelize(list);
		
		JavaRDD<Integer> doubleNum=initRDD.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		
		//不用foreach操作来遍历远程集群上rdd的元素
		//使用collect将远程集群中元素拉取到本地进行遍历，
		//不推荐使用，因为数据量较大的话，由于它要走网络传输，因此性能较差
		//有时还会因为数据量过大出现OOM异常
		List<Integer> collList=doubleNum.collect();
		
		for(Integer i:collList) {
			System.out.println(i);
		}
		context.close();
		
	}
	/**
	 * count操作：返回RDD的length
	 */
	public static void count() {
		SparkConf conf=new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Integer> list=Arrays.asList(1,2,3,4,5);
		//并行化集合，创建初始化RDD
		JavaRDD<Integer> initRDD=context.parallelize(list);
		
		long length=initRDD.count();
		System.out.println(length);
		context.close();
	}
	/**
	 * take操作，与collect类似，不过它是从远程集群上获取指定参数个RDD，而collect是拉取全部元素
	 */
	public static void take() {
		SparkConf conf=new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Integer> list=Arrays.asList(1,2,3,4,5);
		//并行化集合，创建初始化RDD
		JavaRDD<Integer> initRDD=context.parallelize(list);
		
		List<Integer> takeList=initRDD.take(3);
		
		for(Integer i:takeList) {
			System.out.println(i);
		}
		
		context.close();
	}
	/**
	 * 将处理结果保存到文件中
	 */
	public static void saveAsTextFile() {
		SparkConf conf=new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Integer> list=Arrays.asList(1,2,3,4,5);
		//并行化集合，创建初始化RDD
		JavaRDD<Integer> initRDD=context.parallelize(list);

		JavaRDD<Integer> doubleNum=initRDD.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1*2;
			}
		});
		
		doubleNum.saveAsTextFile("file:\\E:\\mycode\\sparkResult");
		context.close();
	}
	public static void countByKey() {
		SparkConf conf=new SparkConf()
				.setAppName("reduce")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		List<Tuple2<String, String>> list=Arrays.asList(
				new Tuple2<String, String>("class1", "marry"),
				new Tuple2<String, String>("class2", "hdfs"),
				new Tuple2<String, String>("class1", "hello"),
				new Tuple2<String, String>("class3", "world"),
				new Tuple2<String, String>("class2", "spark")
				);
		JavaPairRDD<String, String> initRDD=context.parallelizePairs(list);
		
		Map<String, Object> studentsCount=initRDD.countByKey();
		
		for(Map.Entry<String,Object> m:studentsCount.entrySet()) {
			System.out.println(m.getKey()+": "+m.getValue());
		}
		
		context.close();
	}
	
}
