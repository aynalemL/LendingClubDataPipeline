package lendingclub.processors;

import lendingclub.utils.Consts;
import lendingclub.utils.DataUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataCleanerPipe {

    public Dataset<Row> run(Dataset<Row> ds){
        ds = DataUtil.selectColumns(ds, Consts.COLS_BUSINESS_IMPORTANT);
        ds = DataUtil.formatDate(ds, Consts.COL_DATES, Consts.MONTH_YEAR_FORMAT );
        ds = DataUtil.removeNonDigitFromNumericCols(ds);
        ds = DataUtil.convertColDataTypes(ds, Consts.COLS_BUSINESS_IMPORTANT);
        ds = DataUtil.fillMissingWithAverage(ds);
        ds = DataUtil.deleteRowsOrDropCol(ds);

        return ds;
    }

}
