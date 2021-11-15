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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author XIII
 */
public class SparkSQLHBaseApp {

    public static final String TABLE = "stats";
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("data");
    public static final byte[] YEAR_COLUMN = Bytes.toBytes("year");
    public static final byte[] LOCATION_COLUMN = Bytes.toBytes("location");
    public static final byte[] AVG_MIN_TEMP_COLUMN = Bytes.toBytes("avg_min_temp");
    public static final byte[] AVG_MAX_TEMP_COLUMN = Bytes.toBytes("avg_max_temp");
    public static final byte[] AVG_RAINFALL_COLUMN = Bytes.toBytes("avg_rainfall");

    public static void main(String[] args) throws IOException {
        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("SparkSQLHBaseApp");

        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        final SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        sparkSession.sql("drop table if exists weather");

        sparkSession.sql("create external table if not exists " +
                "weather (d string, location string, min_temp double, max_temp double, rainfall double) \n" +
                "row format delimited \n" +
                "fields terminated by ',' \n" +
                "stored as textfile \n" +
                "location 'hdfs:///input'");

        try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
             Admin admin = connection.getAdmin()) {
            final ColumnFamilyDescriptor columnFamily = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY)
                    .setCompressionType(Compression.Algorithm.NONE).build();
            final TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE))
                    .setColumnFamilies(Arrays.asList(columnFamily)).build();

            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
        }

        final JobConf jobConf = new JobConf(HBaseConfiguration.create(), SparkSQLHBaseApp.class);
        jobConf.setOutputFormat(TableOutputFormat.class);
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE);

        final String sql = "select substr(d, 1, 4), location, min_temp, max_temp, rainfall from weather" +
                " where min_temp is not NULL and max_temp is not NULL and rainfall is not NULL";

        sparkSession.sql(sql).javaRDD()
                .mapToPair(r ->
                        new Tuple2<>(
                                new Tuple2(r.getString(0), r.getString(1)),
                                new Tuple4<>(r.getDouble(2), r.getDouble(3), r.getDouble(4), 1)
                        ))
                .reduceByKey((t1, t2) -> new Tuple4<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3(), t1._4() + t2._4()))
                .map(r -> new Tuple5(r._1._1, r._1._2, r._2._1() / r._2._4(), r._2._2() / r._2._4(), r._2._3() / r._2._4()))
                .mapToPair(r -> new Tuple2<>(new ImmutableBytesWritable(), statsDataToPut(r)))
                .saveAsHadoopDataset(jobConf);

        sparkSession.sql("drop table if exists weather");

        sparkContext.close();
    }

    public static Put statsDataToPut(Tuple5<String, String, Double, Double, Double> r) {
        final Put put = new Put(Bytes.toBytes(r._1() + "-" + r._2()));
        put.addColumn(COLUMN_FAMILY, YEAR_COLUMN, Bytes.toBytes(r._1()));
        put.addColumn(COLUMN_FAMILY, LOCATION_COLUMN, Bytes.toBytes(r._2()));
        put.addColumn(COLUMN_FAMILY, AVG_MIN_TEMP_COLUMN, Bytes.toBytes(r._3()));
        put.addColumn(COLUMN_FAMILY, AVG_MAX_TEMP_COLUMN, Bytes.toBytes(r._4()));
        put.addColumn(COLUMN_FAMILY, AVG_RAINFALL_COLUMN, Bytes.toBytes(r._5()));

        return put;
    }
}
