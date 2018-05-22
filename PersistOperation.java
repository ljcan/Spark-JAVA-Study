package cn.just.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化DEMO
 * @author shinelon
 *
 */
public class PersistOperation {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setAppName("persist")
				.setMaster("local");
		JavaSparkContext context=new JavaSparkContext(conf);
		
		
		//持久化主要有cache和persist两种，cache底层是调用了无参的persist函数
		JavaRDD<String> lines=context.textFile("C:/Users/shinelon/Desktop/spark.txt").cache();
		
		long beganTime=System.currentTimeMillis();
		long count=lines.count();
		long endTime=System.currentTimeMillis();
		System.out.println(count);
		System.out.println("cost time is :"+(endTime-beganTime));
		
		beganTime=System.currentTimeMillis();
		count=lines.count();
		endTime=System.currentTimeMillis();
		System.out.println(count);
		System.out.println("cost time is :"+(endTime-beganTime));
		
		context.close();
		
		/**
		 * 没有持久化
		 * 1150080
			cost time is :2279
			
			1150080
			cost time is :858
			
			持久化：
			1150080
			cost time is :3163
			
			1150080
			cost time is :85
			
			结论：对大约100万条数据统计，使用持久化策略比没有持久化策略的情况下性能提升了大约10倍
		 */
	}
}
