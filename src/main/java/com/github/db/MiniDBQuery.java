package com.github.db;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-05-07 14:00
 * @description a pure mini db query tool.
 */
public class MiniDBQuery {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String dbSampleDriverUrl = printDriverAndUrlSample();

    private static final String USAGE =
            "usage: " + LINE_SEPARATOR +
                    " java -jar mini-db-query-all.jar " + LINE_SEPARATOR +
                    "    ---db mysql|oracle|db2" + LINE_SEPARATOR +
                    "    ---driver driver " + LINE_SEPARATOR +
                    "    ---url url " + LINE_SEPARATOR +
                    "    ---user user" + LINE_SEPARATOR +
                    "    ---password password " + LINE_SEPARATOR +
                    "    ---operation select|insert|delete|update " + LINE_SEPARATOR +
                    "    ---sql sql" + LINE_SEPARATOR +
                    "" + LINE_SEPARATOR +
                    "database sample driver and url:" + LINE_SEPARATOR +
                    dbSampleDriverUrl
            ;

    private static void printUsageAndExit(String...messages){
        for (String message : messages) {
            System.err.println(message);
        }
        System.err.println(USAGE);

        System.exit(1);
    }

    private static String getDBParam(String[] args){
        String db = null;
        int index = paramIndexSearch(args,"---db");
        if(index != -1){
            db = args[index+1];
            if(db.equalsIgnoreCase("mysql") ||
               db.equalsIgnoreCase("oracle") ||
               db.equalsIgnoreCase("db2")){
                // TODO
                System.out.println("db type is:" + db);
            }else{
                printUsageAndExit("error: unsupported db:" + db);
            }
        }else{
            printUsageAndExit("error: ---db not found!");
        }
        return db;
    }

    private static String getDriverParam(String[] args){
        String driver = null;
        int index = paramIndexSearch(args,"---driver");
        if(index != -1){
            driver = args[index+1];
            if(driver.trim().isEmpty()){
                printUsageAndExit("error: driver is invalid or empty!");
            }else{
                System.out.println("db driver is:" + driver);
            }
        }else{
            printUsageAndExit("error: ---driver not found!");
        }
        return driver;
    }

    private static String getUrlParam(String[] args){
        String url = null;
        int index = paramIndexSearch(args,"---url");
        if(index != -1){
            url = args[index+1];
            if(url.trim().isEmpty()){
                printUsageAndExit("error: url is invalid or empty!");
            }else{
                System.out.println("db url is:" + url);
            }
        }else{
            printUsageAndExit("error: ---url not found!");
        }
        return url;
    }

    private static String getUserParam(String[] args){
        String user = null;
        int index = paramIndexSearch(args,"---user");
        if(index != -1){
            user = args[index+1];
            if(user.trim().isEmpty()){
                printUsageAndExit("error: user is invalid or empty!");
            }else{
                System.out.println("db login user is:" + user);
            }
        }else{
            printUsageAndExit("error: ---user not found!");
        }
        return user;
    }

    private static String getPasswordParam(String[] args){
        String password = null;
        int index = paramIndexSearch(args,"---password");
        if(index != -1){
            password = args[index+1];
            System.out.println("db login password is:******");
        }else{
            printUsageAndExit("error: ---password not found!");
        }
        return password;
    }

    private static String getOperationParam(String[] args){
        String operation = null;
        int index = paramIndexSearch(args,"---operation");
        if(index != -1){
            operation = args[index+1];
            if(operation.equalsIgnoreCase("select") ||
               operation.equalsIgnoreCase("insert") ||
               operation.equalsIgnoreCase("delete") ||
               operation.equalsIgnoreCase("update")){
                // TODO
                System.out.println("db operation is:" + operation);
            }else{
                printUsageAndExit("error: unsupported operation:" + operation);
            }
        }else{
            printUsageAndExit("error: ---operation not found!");
        }
        return operation;
    }

    private static String getSQLParam(String[] args){
        String sql = null;
        int index = paramIndexSearch(args,"---sql");
        if(index != -1){
            sql = args[index+1];
            if(sql.trim().isEmpty()){
                printUsageAndExit("error: sql is invalid or empty!");
            }else{
                System.out.println("db sql is:" + sql);
            }
        }else{
            printUsageAndExit("error: ---sql not found!");
        }
        return sql;
    }

    /**
     * 查找指定命令行参数的索引位置.
     * @param args 命令行参数数组.
     * @param param 待查找的命令行参数.
     * @return index 参数索引位置,-1表示没有查找到.
     * */
    private static int paramIndexSearch(String[] args,String param){
        int index = -1;
        for (int i = 0; i < args.length; i++) {
            if(args[i].equalsIgnoreCase(param)){
                index = i;
                break;
            }
        }
        return index;
    }

    public static void main(String[] args){
        splitLine();
        // 参数解析
        final String dbType = getDBParam(args);
        final String driver = getDriverParam(args);
        final String url = getUrlParam(args);
        final String user = getUserParam(args);
        final String password = getPasswordParam(args);
        final String operation = getOperationParam(args);
        final String sql = getSQLParam(args);
        splitLine();

        long globalStartTs = System.currentTimeMillis();

        // 加载JDBC驱动
        boolean loadSuccess = DbUtils.loadDriver(driver);
        if(!loadSuccess){
            System.err.println(String.format("database %s driver:%s load failed!",dbType,driver));
            System.exit(1);
        }

        // 连接数据库
        Connection connection = DbUtils.getConnection(url,user,password);
        splitLine();

        // Statement对象参数配置
        StatementConfiguration.Builder builder = new StatementConfiguration.Builder();
        // query timeout seconds
        builder.queryTimeout(180);

        // 创建执行器
        QueryRunner queryRunner = new QueryRunner(builder.build());

        // 结果集解析
        ResultSetHandler<TableListing> handler = new ResultSetHandler<TableListing>() {
            @Override
            public TableListing handle(ResultSet rs) throws SQLException {
                RowSetFactory factory;
                // 离线结果集(避免结果集检索过程中连接断开后数据中断)
                CachedRowSet cachedRowSet = null;
                try{
                    factory = RowSetProvider.newFactory();
                    if(factory != null){
                        cachedRowSet = factory.createCachedRowSet();
                        cachedRowSet.populate(rs);
                    }
                }catch (SQLException se){
                    // ignore
                }

                if(cachedRowSet != null){
                    // 离线ResultSet转TableListing
                    return toTableListing(cachedRowSet);
                }else{
                    // 在线ResultSet转TableListing
                    return toTableListing(rs);
                }
            }
        };

        long queryStartTs = System.currentTimeMillis();

        switch (operation){
            case "select":
            case "insert":
                try{
                    // 执行查询
                    TableListing queryResultListing;
                    if(operation.equalsIgnoreCase("select")){
                        queryResultListing = queryRunner.query(connection,sql,handler);
                    }else{
                        queryResultListing = queryRunner.insert(connection,sql,handler);
                    }

                    long queryMillis = System.currentTimeMillis() - queryStartTs;
                    System.out.println("query runner millis:" + queryMillis);
                    splitLine();

                    // 结果控制台打印
                    System.out.println(queryResultListing.toString());
                }catch (SQLException se){
                    se.printStackTrace();
                }finally {
                    // 关闭数据库连接
                    queryRunner.closeQuietly(connection);
                }
                break;
            case "delete":
            case "update":
                try{
                    int retCode = queryRunner.update(connection,sql);
                    long queryMillis = System.currentTimeMillis() - queryStartTs;
                    System.out.println("query runner millis:" + queryMillis);
                    splitLine();

                    // 结果控制台打印
                    System.out.println(String.format("db operation:%s, affected rows:%s",operation,retCode));
                }catch (SQLException se){
                    se.printStackTrace();
                }finally {
                    // 关闭数据库连接
                    queryRunner.closeQuietly(connection);
                }
                break;
        }

        splitLine();
        System.out.println(String.format("application total millis:%s",(System.currentTimeMillis() - globalStartTs)));
        splitLine();
    }

    /**
     * ResultSet to TableListing.
     * @param rs ResultSet.
     * */
    private static TableListing toTableListing(final ResultSet rs) throws SQLException {
        final ResultSetMetaData rsmd = rs.getMetaData();
        final int cols = rsmd.getColumnCount();

        TableListing.Builder builder = new TableListing.Builder();

        for (int i = 1; i <= cols; i++) {
            String columnName = rsmd.getColumnLabel(i);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(i);
            }
            builder.addField(columnName);
        }

        TableListing tableListing = builder.build();

        while(rs.next()){
            String[] values = new String[cols];
            for(int i = 1; i <= cols; i++){
                String columnValue = rs.getString(i);
                values[i-1] = columnValue;
            }
            tableListing.addRow(values);
        }

        return tableListing;
    }

    private static void splitLine(){
        System.out.println("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -");
    }

    private static String printDriverAndUrlSample(){
        String mysqlSampleDriver = "com.mysql.jdbc.Driver";
        String mysqlSampleUrl = "jdbc:mysql://127.0.0.1:3306/db_name?useUnicode=true&characterEncoding=utf8&useSSL=false";

        String oracleSampleDriver = "oracle.jdbc.driver.OracleDriver";
        String oracleSampleUrl ="jdbc:oracle:thin:@127.0.0.1:1521:db_name";

        String db2SampleDriver = "com.ibm.db2.jcc.DB2Driver";
        String db2SampleUrl = "jdbc:db2://127.0.0.1:50000/db_name";

        StringBuilder sb = new StringBuilder();
        sb.append("    mysql sample driver:");
        sb.append(mysqlSampleDriver);
        sb.append(LINE_SEPARATOR);
        sb.append("    oracle sample driver:");
        sb.append(oracleSampleDriver);
        sb.append(LINE_SEPARATOR);
        sb.append("    db2 sample driver:");
        sb.append(db2SampleDriver);
        sb.append(LINE_SEPARATOR);


        sb.append("    mysql sample url:");
        sb.append(mysqlSampleUrl);
        sb.append(LINE_SEPARATOR);
        sb.append("    oracle sample url");
        sb.append(oracleSampleUrl);
        sb.append(LINE_SEPARATOR);
        sb.append("    db2 sample url:");
        sb.append(db2SampleUrl);
        sb.append(LINE_SEPARATOR);

        return sb.toString();
    }
}
