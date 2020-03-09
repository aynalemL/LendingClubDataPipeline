package lendingclub.processors;

import lendingclub.model.Options;
import lendingclub.utils.Consts;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DataPersister {
    private final SparkSession sparkSession;
    private final Options options;

    public DataPersister(SparkSession sparkSession, Options options) {
        this.sparkSession = sparkSession;
        this.options = options;
    }

    public void persist() {
        persistToDatabase(sparkSession, options);
    }

    private void persistToDatabase(SparkSession sparkSession, Options options) {
        try{
            //schema merging is expensive. For now we don't need. When needed turn  Consts.MERGE_SCHEMA to true
            Dataset<Row> ds = sparkSession.read().option("mergeSchema", Consts.MERGE_SCHEMA).parquet(options.getInputFile());
            Class.forName(options.getDriverName());
            Properties connProperties = createConnProperties(options);
            Class.forName(options.getDriverName());
            ds.write().mode(SaveMode.Append).jdbc(options.getUrl(), options.getTableName(), connProperties);

        }catch (Exception e){
            //TODO handle
        }

    }

    private Properties createConnProperties(Options options) {
        Properties connProperties = new Properties();
        connProperties.put("user",options.getUserName());
        connProperties.put("password", options.getPassword());
        connProperties.put("url", options.getUrl());
        connProperties.put("driver",options.getDriverName());
        connProperties.put("dbname", options.getDBName());
        return connProperties;
    }

}
