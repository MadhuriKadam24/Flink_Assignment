import model.GatewayLog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;
import service.ReportingService;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class ReportingServiceTest {

    @Test
    public void testFilterInput() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSinkString.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("_time,type,incoming_url,microservice","2018-03-12T17:26:04.000-0700,GA.GET,/api-gateway/v3/customers/,API-Gateway")
                .filter(new ReportingService.FilterInput())
                .addSink(new CollectSinkString());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSinkString.values.containsAll(Collections.singleton("2018-03-12T17:26:04.000-0700,GA.GET,/api-gateway/v3/customers/,API-Gateway")));
    }

    @Test
    public void testMapInput() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSinkObjects.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("2018-03-12T17:26:04.000-0700,GA.GET,/api-gateway/v3/customers/,API-Gateway")
                .map(new ReportingService.MapToObjects())
                .addSink(new CollectSinkObjects());

        // execute
        env.execute();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date date = simpleDateFormat.parse("2018-03-12T17:26:04.000-0700");
        GatewayLog log = new GatewayLog(date,"/api-gateway/v3/customers/","GA.GET","API-Gateway");

        // verify your results
        assertTrue(CollectSinkObjects.values.size()>0);
    }

    @Test
    public void testExtractEventTimeAndWatermark() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date date = simpleDateFormat.parse("2018-03-12T17:26:04.000-0700");
        GatewayLog log = new GatewayLog(date,"/api-gateway/v3/customers/","GA.GET","API-Gateway");
        List<GatewayLog> input = new ArrayList<>();
        input.add(log);

        // values are collected in a static variable
        CollectSinkObjects.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromCollection(input)
                .assignTimestampsAndWatermarks(new ReportingService.ExtractTimeStampAndWatermark())
                .addSink(new CollectSinkObjects());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSinkObjects.values.size()>0);
    }

    @Test
    public void testKeyByOperator() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Date date = simpleDateFormat.parse("2018-03-12T17:26:04.000-0700");
        GatewayLog log = new GatewayLog(date,"/api-gateway/v3/customers/","GA.GET","API-Gateway");
        List<GatewayLog> input = new ArrayList<>();
        input.add(log);

        // values are collected in a static variable
        CollectSinkObjects.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromCollection(input)
                .keyBy(new ReportingService.PartitionStreamBySingleKey())
                .addSink(new CollectSinkObjects());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSinkObjects.values.size()>0);
    }

    @Test
    public void testWindowAndApply() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        List<GatewayLog> input = new ArrayList<>();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        Date date = simpleDateFormat.parse("2018-03-12T17:26:04.000-0700");
        GatewayLog log = new GatewayLog(date,"/api-gateway/v3/customers/","GA.GET","API-Gateway");
        input.add(log);

        date = simpleDateFormat.parse("2018-03-12T17:29:42.000-0700");
        log = new GatewayLog(date,"/api-gateway/v2/logs","GA.POST","API-Gateway");
        input.add(log);

        // values are collected in a static variable
        CollectSinkString.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromCollection(input)
                .assignTimestampsAndWatermarks(new ReportingService.ExtractTimeStampAndWatermark())
                .keyBy(new ReportingService.PartitionStreamBySingleKey())
                .window(TumblingEventTimeWindows.of(Time.minutes(1))).apply(new ReportingService.CountServiceCalls())
                .addSink(new CollectSinkString());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSinkString.values.size() == 1);
    }

    @Test
    public void testCompleteWorkFlowPerMinute() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSinkString.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("_time,type,incoming_url,microservice",
                                "2018-03-12T17:26:04.000-0700,GA.GET,/api-gateway/v3/customers/,API-Gateway",
                                "2018-03-12T17:26:04.000-0700,GA.POST,/api-gateway/v2/logs,API-Gateway",
                                "2018-03-12T17:29:42.000-0700,GA.POST,/api-gateway/v2/logs,API-Gateway",
                                "2018-03-12T17:29:38.000-0700,GA.POST,/api-gateway/v2/logs,API-Gateway")
                .filter(new ReportingService.FilterInput())
                .map(new ReportingService.MapToObjects())
                .assignTimestampsAndWatermarks(new ReportingService.ExtractTimeStampAndWatermark())
                .keyBy(new ReportingService.PartitionStreamBySingleKey())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new ReportingService.CountServiceCalls())
                .addSink(new CollectSinkString());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSinkString.values.contains("Key - API-Gateway Window: 1520900820000 -- count: 2"));
    }

    @Test
    public void testCompleteWorkFlowFiveMinutesInterval() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(2);

        // values are collected in a static variable
        CollectSinkString.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("_time,type,incoming_url,microservice",
                                "2018-03-12T17:26:04.000-0700,GA.GET,/api-gateway/v3/customers/,API-Gateway",
                                "2018-03-12T17:26:04.000-0700,GA.POST,/api-gateway/v2/logs,API-Gateway",
                                "2018-03-12T17:29:42.000-0700,GA.POST,/api-gateway/v2/logs,API-Gateway",
                                "2018-03-12T17:40:23.000-0700,GA.POST,/api-gateway/v2/logs,API-Gateway",
                                "2018-03-12T17:43:02.000-0700,GA.GET,/api-gateway/v3/vehicles/3012,API-Gateway")
                .filter(new ReportingService.FilterInput())
                .map(new ReportingService.MapToObjects())
                .assignTimestampsAndWatermarks(new ReportingService.ExtractTimeStampAndWatermark())
                .keyBy(new ReportingService.PartitionStreamByMultipleKeys())
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .apply(new ReportingService.CountServiceCallsGroupByMethod())
                .addSink(new CollectSinkString());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSinkString.values.size() == 2);
    }

    // create a testing sink
    private static class CollectSinkString implements SinkFunction<String> {
        // must be static
        public static final List<String> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value) throws Exception {
            values.add(value);
        }
    }

    // create a testing sink
    private static class CollectSinkObjects implements SinkFunction<GatewayLog> {
        // must be static
        public static final List<GatewayLog> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(GatewayLog value) throws Exception {
            values.add(value);
        }
    }
}
