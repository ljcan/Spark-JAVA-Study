package cn.just.spark.core;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Spark实现二次排序
 * @author shinelon
 * 
 * 测试数据：
 *  1 5
	2 4
	1 3
	1 2
	5 4
	3 4
	3 5
	2 1
	2 5
	4 1
 * 
 * 
 * 排序结果：
 *  5 4
	4 1
	3 5
	3 4
	2 5
	2 4
	2 1
	1 5
	1 3
	1 2
 * 
 * 
 *
 */
public class DoubleSort {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setAppName("DoubleSort")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		JavaRDD<String> javaRDD=context.textFile("C:\\Users\\shinelon\\Desktop\\doubleSort.txt");
		//转换数据格式，比如之前为(1 2)转换为Tuple2(Double(1,2),"1,2"))的格式，前面的第一个参数为自定义的排序key
		JavaPairRDD<DoubleSortKey, String> pairs=javaRDD.mapToPair(
				new PairFunction<String, DoubleSortKey, String>() {
					@Override
					public Tuple2<DoubleSortKey, String> call(String line) throws Exception {
						String[] lines=line.split(" ");
						DoubleSortKey doubleSortKey=new DoubleSortKey(
								Integer.valueOf(lines[0]), 
								Integer.valueOf(lines[1]));
						return new Tuple2<DoubleSortKey, String>(doubleSortKey,line);
					}
		});
		//然后按照key排序
		//由大到小排序
		JavaPairRDD<DoubleSortKey, String> sortPairs=pairs.sortByKey(false);		
		//去除掉自定义的key，然后进行只输出排序好的原始结果
		JavaRDD<String> result=sortPairs.map(new Function<Tuple2<DoubleSortKey,String>, String>() {
			@Override
			public String call(Tuple2<DoubleSortKey, String> v1) throws Exception {
				return v1._2;			//只返回排序后的原始数据，去除掉之前用过的排序key
			}
		});
		//打印结果
		result.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		context.close();
		
		
	}
}