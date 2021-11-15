import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.EsSpark;
import scala.Tuple2;
import scala.Tuple4;

import java.io.IOException;

/**
 * @author XIII
 */
public class SparkSQLEsApp {

    public static void main(String[] args) throws IOException {
        final SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("SparkSQLEsApp");
        sparkConf.set("spark.es.nodes", "localhost");
        sparkConf.set("spark.es.port", "9200");
        sparkConf.set("es.index.auto.create", "true");

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

        final String sql = "select substr(d, 1, 4), location, min_temp, max_temp, rainfall from weather" +
                " where min_temp is not NULL and max_temp is not NULL and rainfall is not NULL";

        final RDD<Stats> result = sparkSession.sql(sql).javaRDD()
                .mapToPair(r ->
                        new Tuple2<>(
                                new Tuple2<>(r.getString(0), r.getString(1)),
                                new Tuple4<>(r.getDouble(2), r.getDouble(3), r.getDouble(4), 1)
                        ))
                .reduceByKey((t1, t2) -> new Tuple4<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3(), t1._4() + t2._4()))
                .map(r -> new Stats(r._1._1, r._1._2, r._2._1() / r._2._4(), r._2._2() / r._2._4(), r._2._3() / r._2._4()))
//                .mapToPair(r -> new Tuple2<>((String) r._1(), r))
                .rdd();

        EsSpark.saveToEs(result, "test/stats");

        sparkSession.sql("drop table if exists weather");

        sparkContext.close();
    }

    public static class Stats {

        private String year;
        private String location;
        private Double avgMinTemp;
        private Double avgMaxTemp;
        private Double avgRainfall;

        public Stats(String year, String location, Double avgMinTemp, Double avgMaxTemp, Double avgRainfall) {
            this.year = year;
            this.location = location;
            this.avgMinTemp = avgMinTemp;
            this.avgMaxTemp = avgMaxTemp;
            this.avgRainfall = avgRainfall;
        }
    }
}

