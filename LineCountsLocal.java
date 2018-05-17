package cn.just.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 统计文本行数
 * @author shinelon
 *
 */
public class LineCountsLocal {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setAppName("LineCountsLocal")
				.setMaster("local");
		JavaSparkContext context=new JavaSparkContext(conf);
		
		JavaRDD<String> lines=context.textFile("C:/Users/shinelon/Desktop/spark02.txt");
		//统计行数，将其转换为（line,1）的形式
		JavaPairRDD<String, Integer> pairs=lines.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		//执行reduceByKey，计算行数
		JavaPairRDD<String, Integer> linePair=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return (v1+v2);
			}
		});
		//执行一个action操作，打印统计行数的次数
		linePair.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+" appears "+t._2);
			}
		});
		//关闭资源连接
		context.close();
	}
}
