import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author XIII
 */
public class SparkStreamingKafkaApp {

    public static final String WEATHER_TABLE = "weather";
    public static final byte[] INFO_COLUMN_FAMILY = Bytes.toBytes("info");
    public static final byte[] DATE_COLUMN = Bytes.toBytes("date");
    public static final byte[] LOCATION_COLUMN = Bytes.toBytes("location");
    public static final byte[] MIN_TEMP_COLUMN = Bytes.toBytes("min_temp");
    public static final byte[] MAX_TEMP_COLUMN = Bytes.toBytes("max_temp");
    public static final byte[] RAINFALL_COLUMN = Bytes.toBytes("rainfall");

    public static void main(String[] args) throws InterruptedException, IOException {
        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("SparkStreamingKafkaApp");

        final JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
             Admin admin = connection.getAdmin()) {
            final ColumnFamilyDescriptor infoColumnFamily = ColumnFamilyDescriptorBuilder.newBuilder(INFO_COLUMN_FAMILY)
                    .setCompressionType(Compression.Algorithm.NONE).build();
            final TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(WEATHER_TABLE))
                    .setColumnFamilies(Arrays.asList(infoColumnFamily)).build();

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
        }

        final Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "group_test");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        final Collection<String> topics = Arrays.asList(args[0]);

        final JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        final JavaPairDStream<ImmutableBytesWritable, Put> data = messages
                .map(ConsumerRecord::value)
                .map(Parser::parse)
                .filter(Objects::nonNull)
                .mapToPair(d -> new Tuple2<>(new ImmutableBytesWritable(), weatherDataToPut(d)));

        final JobConf jobConf = new JobConf(HBaseConfiguration.create(), SparkStreamingKafkaApp.class);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, WEATHER_TABLE);

        data.foreachRDD(rdd -> rdd.saveAsHadoopDataset(jobConf));

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static class Parser {

        public static WeatherData parse(String s) {
            final String[] data = s.trim().split(",");

            if (data[2].trim().equals("NA") || data[3].trim().equals("NA") || data[4].trim().equals("NA"))
                return null;
            else
                return new WeatherData(data[0].trim(), data[1].trim(),
                        Double.parseDouble(data[2].trim()),
                        Double.parseDouble(data[3].trim()),
                        Double.parseDouble(data[4].trim()));
        }
    }

    public static Put weatherDataToPut(WeatherData weatherData) {
        final Put put = new Put(Bytes.toBytes(weatherData.getDate()));
        put.addColumn(INFO_COLUMN_FAMILY, DATE_COLUMN, Bytes.toBytes(weatherData.getDate()));
        put.addColumn(INFO_COLUMN_FAMILY, LOCATION_COLUMN, Bytes.toBytes(weatherData.getLocation()));
        put.addColumn(INFO_COLUMN_FAMILY, MIN_TEMP_COLUMN, Bytes.toBytes(weatherData.getMinTemp()));
        put.addColumn(INFO_COLUMN_FAMILY, MAX_TEMP_COLUMN, Bytes.toBytes(weatherData.getMaxTemp()));
        put.addColumn(INFO_COLUMN_FAMILY, RAINFALL_COLUMN, Bytes.toBytes(weatherData.getRainfall()));

        return put;
    }

    public static class WeatherData {

        private String date;
        private String location;
        private double minTemp;
        private double maxTemp;
        private double rainfall;

        public WeatherData(String date, String location, double minTemp, double maxTemp, double rainfall) {
            this.date = date;
            this.location = location;
            this.minTemp = minTemp;
            this.maxTemp = maxTemp;
            this.rainfall = rainfall;
        }

        public String getDate() {
            return date;
        }

        public String getLocation() {
            return location;
        }

        public double getMinTemp() {
            return minTemp;
        }

        public double getMaxTemp() {
            return maxTemp;
        }

        public double getRainfall() {
            return rainfall;
        }
    }
}
