package lendingclub.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DataUtil {

    public static Dataset<Row> selectColumns(Dataset<Row> ds, String[] cols) {
        ds = ds.select("term", cols);
        return ds;
    }
    public static Dataset<Row> removeColumns(Dataset<Row> ds, String [] cols){
        ds = ds.drop(cols);
        return ds;
    }
    public static Dataset<Row> replaceColText(Dataset<Row> ds, String colName, String pattern, String val){
         ds = ds.withColumn(colName, functions.regexp_replace(ds.col(colName), pattern,val));
         return ds;
    }
    public static Dataset<Row> removeNonDigitFromNumericCols(Dataset<Row> ds){
       List<String> integerFields  = new ArrayList<>();
       for(String col : Consts.COLS_DATATYPE_MAP.keySet())
           if(Consts.COLS_DATATYPE_MAP.get(col).typeName().equals("int")){
               integerFields.add(col);
        }
        ds = removeNonDigit(ds, (String[])integerFields.toArray());
       return  ds;
    }
    public static Dataset<Row> removeNonDigit(Dataset<Row> ds, String [] colNames){
        for(String colName:colNames){
            ds = ds.withColumn(colName, functions.regexp_replace(ds.col(colName), "\\D*",""));
        }
        return ds;
    }
    public static Dataset<Row> fillMissingWithAverage(Dataset<Row> ds){
        for(String colName : Consts.COLS_DATATYPE_MAP.keySet()) {
            String type = Consts.COLS_DATATYPE_MAP.get(colName).typeName();
            if ("integer".equals(type) || "double".equals(type) || "float".equals(type)) {
               ds = fillMissingWithAverage(ds,colName);
            }
        }
        return ds;
    }
    public static Dataset<Row> fillMissingWithAverage(Dataset<Row> ds, String colName){
        double avgEmpLen = columnAvg(ds, colName);
        ds = ds.withColumn(colName,when(col(colName).isNaN(), avgEmpLen).otherwise(col(colName)));
        return ds;
    }
    public static double columnAvg(Dataset<Row> ds, String colName){
      return ((Double) ds.agg(avg(colName)).first().get(0));
    }
    public static Dataset<Row> formatDate(Dataset<Row>ds, String  [] colNames, String format){
       for(String colName : colNames){
           ds = ds.withColumn(colName, to_date(ds.col(colName),format));
       }
        return ds;
    }
    public static Dataset<Row> convertColDataTypes(Dataset<Row> ds, String[]colNames){
        for(String colName: colNames){
            if(Consts.COLS_DATATYPE_MAP.containsKey(colName)){
                ds = ds.withColumn(colName, ds.col(colName).cast(Consts.COLS_DATATYPE_MAP.get(colName)));
            }
        }
        return ds;
    }
    public static Dataset<Row> deleteRowsOrDropCol(Dataset<Row> ds){
        for(String colName : Consts.COLS_BUSINESS_IMPORTANT){
            boolean isSparse = computeNullRatio(ds, colName) > Consts.COL_PRESEVATION_THRESHOLD ? true : false;
            ds = deleteRowsOrDropCol(ds, colName, isSparse);
        }
        return ds;
    }
    public static Dataset<Row> deleteRowsOrDropCol(Dataset<Row> ds, String colName, boolean isSparse){
        if(isSparse){
           ds = ds.drop(colName);
        }else{
           ds = deleteRowsWithNull(ds, colName);
        }
        return ds;
    }

    public static float computeNullRatio(Dataset<Row> ds, String colName) {
        long nullCount = countColNulls(ds, colName);
        long totalCount = ds.count();
        return (float) (nullCount/totalCount);
    }

    public static long countColNulls(Dataset<Row>ds, String colName){
        return ds.filter(col(colName).isNull()).count();
    }
    public static Dataset<Row> deleteRowsWithNull(Dataset<Row> ds, String colName){
        ds = ds.filter(col(colName).isNotNull());
        return ds;
    }
    public static Dataset<Row> dropCols(Dataset<Row> ds, String [] colNames){
        ds = ds.drop(colNames);
        return ds;
    }
}
