package lendingclub.model;

public class Options {
    private String driverName;
    private String format;
    private String url;
    private String tableName;
    private String userName;
    private String password;
    private String sparkMaster;
    private String inputFile;
    private String outputFile;
    private String dbName;

    private Options(String driverName, String format, String url, String dbName, String tableName, String userName, String password, String sparkMaster,String inputFile, String outputFile) {
        this.driverName = driverName;
        this.format = format;
        this.url = url;
        this.tableName = tableName;
        this.userName = userName;
        this.password = password;
        this.sparkMaster = sparkMaster;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.dbName = dbName;
    }

    public static class DbOptionsBuilder {
        private String driverName;
        private String format;
        private String url;
        private String dbName;
        private String tableName;
        private String userName;
        private String password;
        private String sparkMaster;
        private String inputFile;
        private String outputFile;

        public DbOptionsBuilder withDriver(String driverName){
            this.driverName = driverName;
            return this;
        }
        public DbOptionsBuilder withFormat(String format){
            this.format = format;
            return this;
        }
        public DbOptionsBuilder withUrl(String url){
            this.url = url;
            return this;
        }
        public DbOptionsBuilder withDbName(String url){
            this.dbName = dbName;
            return this;
        }
        public DbOptionsBuilder withTable(String tableName){
            this.tableName = tableName;
            return this;
        }
        public DbOptionsBuilder withUser(String userName){
            this.userName = userName;
            return this;
        }
        public DbOptionsBuilder withPassword(String password){
            this.password = password;
            return this;
        }
        public DbOptionsBuilder withSparkMaster(String sparkMaster){
            this.sparkMaster = sparkMaster;
            return this;
        }
        public DbOptionsBuilder withInputFile(String inputFile){
            this.inputFile = inputFile;
            return this;
        }
        public DbOptionsBuilder withOutputFile(String outputFile){
            this.outputFile = outputFile;
            return this;
        }

        public Options build(){
            return new Options(this.driverName, this.format,this.url,this.dbName, this.tableName, this.userName,this.password, this.sparkMaster, this.inputFile, this.outputFile);
        }
    }

    public String getDriverName() {
        return driverName;
    }

    public String getFormat() {
        return format;
    }

    public String getUrl() {
        return url;
    }

    public String getDBName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public String getInputFile() {
        return inputFile;
    }

    public String getOutputFile() {
        return outputFile;
    }
}
