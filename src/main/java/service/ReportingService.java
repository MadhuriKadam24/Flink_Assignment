package service;


import model.GatewayLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ReportingService {
	
	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		String path = "src/main/resources/gateway_logs.csv";
		TextInputFormat format = new TextInputFormat(new Path(path));

		// Reading input data
		DataStream<String> inputStream = env.readFile(format, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 100);

		// Obtaining useful DataStream from input data, which can be useful for further processing
		SingleOutputStreamOperator<String> filteredStream = inputStream.filter(new FilterInput());

		SingleOutputStreamOperator<GatewayLog> logStream = filteredStream.map(new MapToObjects());

		// Extracting event time and watermark
		DataStream<GatewayLog> withTimestampsAndWatermarks = logStream.assignTimestampsAndWatermarks(new ExtractTimeStampAndWatermark());

		KeyedStream<GatewayLog, String> keyedStreamSingleKey = withTimestampsAndWatermarks.keyBy(new PartitionStreamBySingleKey());

		//1 : Aggregate count of API calls made to the gateway service per minute
		DataStream<String> result1 = keyedStreamSingleKey.window(TumblingEventTimeWindows.of(Time.minutes(1))).apply(new CountServiceCalls());
		result1.writeAsText("src/main/resources/countOfAPIPerMinute");

		//2 : Aggregate count of API calls made to the gateway service every 5 minutes
		DataStream<String> result2 = keyedStreamSingleKey.window(TumblingEventTimeWindows.of(Time.minutes(5))).apply(new CountServiceCalls());
		result2.writeAsText("src/main/resources/countOfAPIPer5Minutes");

		// Key by multiple fields such as by microservice and type of HTTP API
		KeyedStream<GatewayLog, Tuple2<String, String>> keyedStreamByMultipleKeys = withTimestampsAndWatermarks.keyBy(new PartitionStreamByMultipleKeys());

		//3 : Aggregate count of API calls made to the service grouped by HTTP method per minute
		DataStream<String> result3 = keyedStreamByMultipleKeys.window(TumblingEventTimeWindows.of(Time.minutes(1))).apply(new CountServiceCallsGroupByMethod());
		result3.writeAsText("src/main/resources/countOfAPIPerMinuteByHttpMethods");

		//4 : Aggregate count of API calls made to the service grouped by HTTP method every 5 minutes
		DataStream<String> result4 = (DataStream<String>) keyedStreamByMultipleKeys.window(TumblingEventTimeWindows.of(Time.minutes(5))).apply(new CountServiceCallsGroupByMethod());
		result4.writeAsText("src/main/resources/countOfAPIPer5MinutesByHttpMethods");

		// execute program
		env.execute("Gateway log reporting system");
	}

	public static class CountServiceCallsGroupByMethod implements WindowFunction<GatewayLog, String, Tuple2<String, String>, TimeWindow> {
		@Override
		public void apply(Tuple2<String, String> stringStringTuple2, TimeWindow window, Iterable<GatewayLog> input, Collector<String> out) throws Exception {
			int count = 0;
			for (GatewayLog i : input) {
				count++;
			}
			String key = stringStringTuple2.f0 + ":" + stringStringTuple2.f1;
			out.collect("Key - " + key + " Window: " + window.getEnd() + " -- count: " + count);
		}
	}


	public static class CountServiceCalls implements WindowFunction<GatewayLog, String, String, TimeWindow> {
		@Override
		public void apply(String s, TimeWindow window, Iterable<GatewayLog> input, Collector<String> out) throws Exception {
			int count = 0;
			for (GatewayLog i:input) {
				count++;
			}
			out.collect("Key - "+ s + " Window: " + window.getEnd() + " -- count: " + count);
		}
	}

	public static class FilterInput implements FilterFunction<String> {
		@Override
		public boolean filter(String value) throws Exception {
			if (value.contains("_time")) return false;
			else return true;
		}
	}

	public static class MapToObjects implements MapFunction<String, GatewayLog> {
		@Override
		public GatewayLog map(String value) throws Exception {
			String[] values = value.split(",");
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
			Date date = simpleDateFormat.parse(values[0]);
			return new GatewayLog(date, values[2], values[1], values[3]);
		}
	}

	public static class ExtractTimeStampAndWatermark implements AssignerWithPeriodicWatermarks<GatewayLog> {
		private long currentMaxTimestamp;
			@Override
			public long extractTimestamp(GatewayLog element, long previousElementTimestamp) {
				long timestamp = element.getTimestamp().getTime();
				currentMaxTimestamp = Math.max(timestamp, previousElementTimestamp);
				return timestamp;
			}

			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				final long maxOutOfOrderness = 3500; // 3.5 seconds
				return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
			}
	}

	public static class PartitionStreamBySingleKey implements KeySelector<GatewayLog,String> {
		@Override
		public String getKey(GatewayLog value) throws Exception {
			return value.getMicroservice();
		}
	}

	public static class PartitionStreamByMultipleKeys implements KeySelector<GatewayLog, Tuple2<String, String>>{
		@Override
		public Tuple2<String, String> getKey(GatewayLog value) throws Exception {
			return Tuple2.of(value.getMicroservice(),value.getType());
		}
	}
}
