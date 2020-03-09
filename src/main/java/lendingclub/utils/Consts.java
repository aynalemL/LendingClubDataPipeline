package lendingclub.utils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.*;

public class Consts {
    //Based on exploratory analysis of the row input data these are columns with more than 75% null/empty values.
    public static final String [] COLS_TO_DROP = new String []{"id", "member_id", "url", "desc", "mths_since_last_record", "annual_inc_joint", "dti_joint", "verification_status_joint", "mths_since_recent_bc_dlq", "revol_bal_joint", "sec_app_earliest_cr_line", "sec_app_inq_last_6mths", "sec_app_mort_acc", "sec_app_open_acc", "sec_app_revol_util", "sec_app_open_act_il", "sec_app_num_rev_accts", "sec_app_chargeoff_within_12_mths", "sec_app_collections_12_mths_ex_med", "sec_app_mths_since_last_major_derog", "hardship_type", "hardship_reason", "hardship_status", "deferral_term", "hardship_amount", "hardship_start_date", "hardship_end_date", "payment_plan_start_date", "hardship_length", "hardship_dpd", "hardship_loan_status", "orig_projected_additional_accrued_interest", "hardship_payoff_balance_amount", "hardship_last_payment_amount", "debt_settlement_flag_date", "settlement_status", "settlement_date", "settlement_amount", "settlement_percentage", "settlement_term"};
    //The following columns are selected to be of important business interest based on exploratory analysis of the row input data
    public static final String [] COLS_BUSINESS_IMPORTANT = new String []{"term","loan_status","annual_inc","purpose","dti","delinq_amnt","dti_joint","emp_title","emp_length","home_ownership","annual_inc","issue_d","int_rate","delinq_2yrs","last_pymnt_d","next_pymnt_d","policy_code","application_type","annual_inc_joint","acc_now_delinq","delinq_amnt","grade","title","recoveries","last_pymnt_d","next_pymnt_d","acc_now_delinq","tot_cur_bal"};
    //Expected datatype for based understanding from exploratory analysis of the row input data
    public static final Map<String, DataType> COLS_DATATYPE_MAP =  new HashMap<String, DataType>();
    public static final float COL_PRESEVATION_THRESHOLD = 0.25f;
    public static final String WRITE_FORMAT = "jdbc";
    public static final boolean MERGE_SCHEMA = false;
    public static final String MONTH_YEAR_FORMAT = "MMM-yyyy";
    public static final String OUTPUT_TABLE_NAME = "LOANDATA";
    public static final String DB_DRIVER_NAME = "org.postgresql.Driver";
    public static final String[] COL_DATES = new String[]{"issue_d","last_pymnt_d","next_pymnt_d"};
    public static final Map<String, Set<String>> COLS_CATEGORICAL = new HashMap<>();


    static {
        COLS_DATATYPE_MAP.put("term",DataTypes.IntegerType );
        COLS_DATATYPE_MAP.put("loan_status",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("annual_inc",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("purpose",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("dti",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("delinq_amnt",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("dti_joint",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("emp_title",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("emp_length",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("home_ownership",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("annual_inc",DataTypes.IntegerType );
        COLS_DATATYPE_MAP.put("issue_d",DataTypes.DateType );
        COLS_DATATYPE_MAP.put("int_rate",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("delinq_2yrs",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("last_pymnt_d",DataTypes.DateType );
        COLS_DATATYPE_MAP.put("next_pymnt_d",DataTypes.DateType );
        COLS_DATATYPE_MAP.put("policy_code",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("application_type",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("annual_inc_joint",DataTypes.IntegerType );
        COLS_DATATYPE_MAP.put("acc_now_delinq",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("delinq_amnt",DataTypes.DoubleType );
        COLS_DATATYPE_MAP.put("grade",DataTypes.StringType );
        COLS_DATATYPE_MAP.put("title",DataTypes.StringType);
        COLS_DATATYPE_MAP.put("recoveries",DataTypes.IntegerType );
        COLS_DATATYPE_MAP.put("tot_cur_bal",DataTypes.DoubleType );
    }

}

