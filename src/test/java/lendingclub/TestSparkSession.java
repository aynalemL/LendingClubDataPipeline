package lendingclub;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class TestSparkSession {
    private static SparkSession spark;

    public TestSparkSession() {
        spark = SparkSession.builder()
                .master("local[*]")
                .config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(TestSparkSession.class.getName())
                .getOrCreate();
    }

    public static SparkSession instance(){
        if(spark == null) {
             new TestSparkSession();
             return spark;
        }
        return spark;
    }
}
