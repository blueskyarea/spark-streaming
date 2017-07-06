package com.blueskyarea;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class StreamingGetFileName {

	public static void main(String[] args) {
		// create StreamingContext
		SparkConf conf = new SparkConf().setAppName("StreamingGetFileName");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		// create DStream from text file
		String logDir = "/tmp/logs";
		JavaDStream<String> logData = jssc.textFileStream(logDir);
		
		// try to get file name
		logData.foreachRDD(new Function<JavaRDD<String>, Void>() {

			private static final long serialVersionUID = -8895146405764617651L;

			public Void call(JavaRDD<String> rdd) throws Exception {
				String regex = "file:.*";
				Pattern p = Pattern.compile(regex);
				Matcher m = p.matcher(rdd.toDebugString());
				if (m.find()){
					System.out.println(m.group().split("\\s+")[0]);
				}
				return null;
			}
			
		});
		
		// start streaming
		jssc.start();
		        
		// wait for end of job
		jssc.awaitTermination();
	}

}
