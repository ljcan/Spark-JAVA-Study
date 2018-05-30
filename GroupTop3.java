package cn.just.spark.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 按照班级分组取每个班级成绩的前三名
 * @author shinelon
 * 
 * 测试数据如下：
 *  class1 85
	class2 56
	class1 78
	class1 89
	class1 85
	class1 87
	class2 96
	class2 95
	class2 97
 *
 *运行结果如下：
 *  class1 : 
	89
	87
	85
	class2 : 
	97
	96
	95
 */
public class GroupTop3 {
	public static void main(String[] args) {
		SparkConf conf=new SparkConf()
				.setAppName("GroupTop3")
				.setMaster("local");
		JavaSparkContext context=new JavaSparkContext(conf);
		
		JavaRDD<String> rdd=context.textFile("C:\\Users\\shinelon\\Desktop\\score.txt");
		//将字符串转换为元组对
		JavaPairRDD<String, Integer> pairRDD=rdd.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String[] line=t.split(" ");
				return new Tuple2<String, Integer>(line[0], Integer.valueOf(line[1]));
			}
		});
		//按照班级分组
		JavaPairRDD<String, Iterable<Integer>> groupRDD=pairRDD.groupByKey();
		
		//取每一个班级成绩的前三名
		JavaPairRDD<String, Iterable<Integer>> top3RDD=groupRDD.mapToPair(
				new PairFunction<Tuple2<String,Iterable<Integer>>, String, Iterable<Integer>>() {
			//获取前三名的算法
			@Override
			public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				String className=t._1;
				Integer[] top3Scores=new Integer[3];
				for(int i=0;i<3;i++) {
					Iterator<Integer> scores=t._2.iterator();
					top3Scores[i]=0;
					while(scores.hasNext()) {
						List<Integer> l=Arrays.asList(top3Scores);
						int next=scores.next();
						if(i==0&&top3Scores[i]<next) {
							top3Scores[i]=next;
						}else if(i!=0&&!l.contains(next)&&next>top3Scores[i]){
							top3Scores[i]=next;
						}
//							else if(i==1&&next!=top3Scores[i-1]&&next>top3Scores[i]) {
//							top3Scores[i]=next;
//						}else if(i==2&&next!=top3Scores[i-1]&&next!=top3Scores[i-2]&&next>top3Scores[i]) {
//							top3Scores[i]=next;
//						}
					}
				}
				return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3Scores));
			}
		});
		
		top3RDD.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println(t._1+" : ");
				for(Integer score:t._2) {
					System.out.println(score);
				}
			}
		});
		
		context.close();
	}
}
