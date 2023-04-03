import Operation.Insertion;
import PO.RawData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;


import java.sql.SQLException;
import java.util.Map;

public class MyClickHouseLink extends RichSinkFunction<RawData> {
    public static final String HOST = "localhost";
    private String username = "default";
    private String password = "root";
    public static long count = 0;

    public static long maxTxtLine = 26010522 - 3000; // txt file lines - 3000.
    private static final long serialVersionUID = 1L;
    private static final String MAX_PARALLEL_REPLICAS_VALUE = "2";
    protected ClickHouseConnection conn;
    private ClickHouseStatement stmt;

    public MyClickHouseLink() {}


    //open clickhouse
    @Override
    public void open(Configuration parameters) {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(username);
        properties.setPassword(password);
        properties.setSessionId("default-session-id");
        BalancedClickhouseDataSource dataSource;
        try {
            if (null == conn) {
                //source
                dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://"+ HOST+":8123", properties);
                conn = dataSource.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //SskData-------->InputStream, add to clickhouse
    @Override
    public void invoke(RawData value, Context context) throws Exception {
        //InputStream result = new ByteArrayInputStream(value.getEventBody().getBytes(StandardCharsets.UTF_8));//result干嘛用？
        Insertion insertion = new Insertion();
        String sql = insertion.resolveSql(value);
        try{
            count++;
            stmt = conn.createStatement();
            Map map = insertion.getSqlMap();
            if (count > maxTxtLine || count % 3000 == 0) {
                String multiInsertSql = insertion.getMultiInsertionSql();
                System.out.println("Sql " + count +": ");
                if (!multiInsertSql.equals("")) {
                    stmt.executeQuery(multiInsertSql);
                    System.out.println(multiInsertSql.substring(multiInsertSql.indexOf("VALUES")));
                }
            }
        }catch (Exception e){
            count++;
            if (count % 1000 == 0) {
                System.out.println("Error " + count +": " + sql);
                System.out.println("eventBody:" + value.getEventBody());
                e.printStackTrace();
            }
        }
    }

    //close connection
    @Override
    public void close() throws Exception {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}