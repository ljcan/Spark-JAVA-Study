package cn.just.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.Accumulators;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;


/**
 * 共享变量测试
 * @author shinelon
 *
 */
public class BroadCastVariables {
	public static void main(String[] args) {
//		BradCastVariable();
		accumlatorVariable();
	}
	/**
	 * 使用广播变量是在每一个节点拷贝一份缓存而不是每一个节点的task，因此降低了内存的消耗
	 * 广播变量只读，不可修改
	 */
	public static void BradCastVariable() {
		SparkConf conf=new SparkConf()
				.setAppName("broadcast")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		final int factor=2;
		final Broadcast<Integer> broadFactor=context.broadcast(factor);
		List<Integer> list=Arrays.asList(1,2,3,4);
		
		JavaRDD<Integer> initRDD=context.parallelize(list);
		
		JavaRDD<Integer> mapRDD=initRDD.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				//通过value方法获取广播变量的值
				int value=broadFactor.value();
				return v1*factor;
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
	 * 累加变量类似于一个累加器，可以在集群中运行任务时进行累加操作，但是不能读取，值允许driver读取
	 */
	public static void accumlatorVariable() {
		SparkConf conf=new SparkConf()
				.setAppName("accumlatorVariable")
				.setMaster("local");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		final Accumulator<Integer> acc=context.accumulator(0);
		
		List<Integer> list=Arrays.asList(1,2,3,4);
		
		JavaRDD<Integer> initRDD=context.parallelize(list);
		
		initRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer t) throws Exception {
				//对累加变量进行累加
				acc.add(t);
			}
		});
		System.out.println(acc.value());
		context.close();
	}
}
