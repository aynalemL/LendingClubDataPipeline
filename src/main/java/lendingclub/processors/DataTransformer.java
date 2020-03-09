package lendingclub.processors;

import lendingclub.model.Options;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DataTransformer {
    private final SparkSession sparkSession;
    private final Options options;
    private DataCleanerPipe dataCleanerPipe = new DataCleanerPipe();

    public DataTransformer(SparkSession sparkSession, Options options) {
        this.sparkSession = sparkSession;
        this.options = options;
    }

    public void transform() {
        Dataset<Row> ds = loadDatasetFrom(options.getInputFile());
        ds = cleanData(ds);
        writeToParquet(ds, options.getOutputFile());
    }

    private  Dataset<Row> loadDatasetFrom(String inputFile){
        return sparkSession.read().format("csv")
                .option("inferSchema",true)
                .option("header", true)
                .option("sep",",")
                .load(inputFile);
    }

    private Dataset<Row> cleanData(Dataset<Row> ds) {
        dataCleanerPipe.run(ds);
        return ds;
    }

    private void writeToParquet(Dataset<Row> ds, String outputFile) {
        deleteDir(outputFile);
        ds.write().option("header",true).parquet(outputFile);
    }

    private  void deleteDir(String outputFilePath) {
        try {
            if ( Files.exists(Paths.get(outputFilePath))){
                FileUtils.deleteDirectory(new File(outputFilePath));
            }
        }   catch (IOException x) {
            System.err.println(x);
        }
    }
}

