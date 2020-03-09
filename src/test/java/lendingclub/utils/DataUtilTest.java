package lendingclub.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.*;

public class DataUtilTest extends BaseTest {
    private DataUtil cleaner = new DataUtil();
    @Test
    public void testRemoveColumns(){
        //before
        int numOfColsBefore = ds.columns().length;
        //action
        ds = cleaner.removeColumns(ds, Consts.COLS_TO_DROP);
        //after
        int numOfColsAfter = ds.columns().length;
        assertEquals(numOfColsBefore, Consts.COLS_TO_DROP.length + numOfColsAfter);
    }
    @Test
    public void testReplaceColText(){
        //before
        assertEquals(" months", ds.select("term").head().get(0).toString().substring(3));
        assertFalse(StringUtils.isNumeric(ds.select("term").head().get(0).toString().trim()));
       //action
        ds = cleaner.replaceColText(ds,"term"," months","");
       //after
        assertEquals("", ds.select("term").head().get(0).toString().substring(3));
        assertTrue(StringUtils.isNumeric(ds.select("term").head().get(0).toString().trim()));
    }

    @Test
    public void testRemoveNonDigit(){
        //before
        assertFalse(StringUtils.isNumeric(ds.select("emp_length").head().get(0).toString().trim()));
        //action
        ds = cleaner.removeNonDigit(ds, new String[]{"emp_length"});
        //after clean
        assertTrue(StringUtils.isNumeric(ds.select("emp_length").head().get(0).toString().trim()));
    }
    @Test
    public void testFillMissingWithAverage() throws Exception{
        //before
        Dataset<Row> dataset = generateDummyDataset();
        dataset = cleaner.convertColDataTypes(dataset,new String[]{"annual_inc"});
        dataset.createOrReplaceTempView("view");
        long missingCount = sparkSession.sql("select annual_inc from view where annual_inc is null").count();
        assertTrue(missingCount == 1 );
        assertEquals(3, dataset.count());
        //action
        dataset = cleaner.fillMissingWithAverage(dataset, "annual_inc");
        //after
        dataset.createOrReplaceTempView("view");
        missingCount = sparkSession.sql("select emp_length from view where trim(emp_length) = ''").count();
        assertTrue(missingCount == 0);
        assertEquals(3, dataset.count());
    }
    @Test
    public void testColumnAvg(){
        Dataset<Row> dataset = generateDummyDataset();
        Double avg = cleaner.columnAvg(dataset,"emp_length");
        assertTrue(avg.equals(15.0));
    }

    @Test
    public void testFormatDate(){
        String [] colNames = new String []{"issue_d"};
        //before
        assertTrue(ds.select(colNames[0]).schema().fields()[0].dataType().typeName().equals("string"));
        //action
        ds = cleaner.formatDate(ds,colNames, "MMM-yyyy");
        //after
        assertTrue(ds.select(colNames[0]).schema().fields()[0].dataType().typeName().equals("date"));
    }

    @Test
    public void testConvertColDataTypes(){
        Dataset<Row> dataset = generateDummyDataset();
        //before
        assertTrue(dataset.select("annual_inc").schema().fields()[0].dataType().typeName().equals("string"));
        //action
        dataset = cleaner.convertColDataTypes(dataset,new String[]{"annual_inc"});
        //after
        assertTrue(dataset.select("annual_inc").schema().fields()[0].dataType().typeName().equals("integer"));
    }

    @Test
    public void testDeleteRowsOrDropCol(){
        Dataset<Row> dataset = generateDummyDataset();
        dataset = cleaner.convertColDataTypes(dataset, new String[]{"annual_inc"});
        //before
        long nullRowCount = cleaner.countColNulls(dataset, "annual_inc");
        long totalRowCount = dataset.count();
        assertEquals(1, nullRowCount);
        assertEquals(3, totalRowCount);
        //action
        dataset = cleaner.deleteRowsOrDropCol(dataset,"annual_inc", false);
        //after
        nullRowCount = cleaner.countColNulls(dataset, "annual_inc");
        totalRowCount = dataset.count();
        assertEquals(0, nullRowCount);
        assertEquals(2, totalRowCount);

    }
    private Dataset<Row> generateDummyDataset() {
        List<String> data = new ArrayList<String>();
        data.add("row1, 10");
        data.add("100000, 20");
        data.add("200000, 15");
        Dataset<Row> dataset = sparkSession.createDataset(data, Encoders.STRING()).toDF();
        dataset = dataset.selectExpr("split(value, ',')[0] as annual_inc", "split(value, ',')[1] as emp_length");
        return dataset;
    }

}


