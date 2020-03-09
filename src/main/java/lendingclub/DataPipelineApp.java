package lendingclub;

import lendingclub.model.Options;
import lendingclub.processors.DataPersister;
import lendingclub.processors.DataTransformer;
import lendingclub.utils.Consts;
import org.apache.spark.sql.SparkSession;

public class DataPipelineApp {

    public static void main(String[] args) {
        final Options options = buildDbOptions(args);
        final SparkSession sparkSession = getOrCreateSparkSession(options.getSparkMaster());
        new DataTransformer(sparkSession, options).transform();
        new DataPersister(sparkSession, options).persist();
        sparkSession.close();
    }

    private static Options buildDbOptions(String[] args) {
        return new Options.DbOptionsBuilder()
                    .withSparkMaster(args[0])
                    .withInputFile(args[1])
                    .withOutputFile(args[2])
                    .withUrl(args[3])
                    .withTable(Consts.OUTPUT_TABLE_NAME)
                    .withDriver(Consts.DB_DRIVER_NAME)
                    .withFormat(Consts.WRITE_FORMAT)
                    .withUser(args[4])
                    .withPassword(args[5])
                    .build();
    }

    private static SparkSession getOrCreateSparkSession(String sparkMaster) {
        return SparkSession.builder()
                .master(sparkMaster)
                .appName(DataPipelineApp.class.getName())
                .getOrCreate();
    }
}
