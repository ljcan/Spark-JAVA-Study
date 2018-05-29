package cn.just.spark.core;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 使用Spark实现topn问题，即取出数据集中最大的前几个数据
 * 这里取出最大的前三个
 * @author shinelon
 *
 */
public class Top3 {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setAppName("Top3")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		JavaRDD<String> rdd=context.textFile("C:\\Users\\shinelon\\Desktop\\top3.txt");
		
		JavaPairRDD<Integer, String> pairRDD=rdd.mapToPair(new PairFunction<String, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				return new Tuple2<Integer, String>(Integer.valueOf(t), t);
			}
		});
		//排序
		JavaPairRDD<Integer, String> sortRDD=pairRDD.sortByKey(false);
		
		JavaRDD<String> result=sortRDD.map(new Function<Tuple2<Integer,String>, String>() {

			@Override
			public String call(Tuple2<Integer, String> v1) throws Exception {
				return v1._2;
			}
		});
		//取最大的前三个
		List<String> list=result.take(3);
		
		for(String str:list) {
			System.out.println(str);
		}
		
		context.close();
		
		
	}
}
