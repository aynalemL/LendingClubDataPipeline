package lendingclub.utils;

import lendingclub.TestConstants;
import lendingclub.TestSparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;

public class BaseTest {
     static SparkSession sparkSession;
    static Dataset<Row> ds;
    @BeforeClass
    public static void beforeClass() {
        sparkSession = TestSparkSession.instance();
        ds = sparkSession.read().format("csv")
                .option("inferSchema",true)
                .option("header", true)
                .option("sep",",")
                .load(TestConstants.TEST_INPUT_DATA);
    }

    @AfterClass
    public static void afterClass() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}