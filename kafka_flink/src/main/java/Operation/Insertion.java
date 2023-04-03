package Operation;

import PO.RawData;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Insertion {
    public static Map<String, ArrayList<String>> sqlMap = new HashMap<>();

    public Map<String, ArrayList<String>> getSqlMap() {
        return sqlMap;
    }
    public Insertion(){}

    private static Insertion multiInsertion;

    private int rowValueNum = 3000; // sqlMap multi insert row number.



    public void putSqlToMap(String eventType, String sql) {
        int valueIndex = sql.indexOf("VALUES");
        String value = sql.substring(valueIndex + 6);
        if (!sqlMap.containsKey(eventType)) {
            ArrayList<String> temp = new ArrayList<>();
            String insert = sql.substring(0, valueIndex + 6);
            temp.add(insert);
            sqlMap.put(eventType, temp);
        }
        sqlMap.get(eventType).add(value);
    }

    public String hasEnoughSql() {
        // return type which has row values more than this.rowValueNum. None for "".
        for (String eachKey : sqlMap.keySet()) {
            if (sqlMap.get(eachKey).size() > rowValueNum) {
                return eachKey;
            }
        }
        return "";
    }

    public String findMaxValuesSql() {
        String maxLenKey = "";
        int maxLen = 0;
        for (String eachKey : sqlMap.keySet()) {
            if (sqlMap.get(eachKey).size() > maxLen) {
                maxLen = sqlMap.get(eachKey).size();
                maxLenKey = eachKey;
            }
        }
        if (maxLen <= 1) return "";
        return maxLenKey;
    }

    public String getMultiInsertionSql() {
        String mapKey = findMaxValuesSql(); // find max value and make it become multiInsertion.
        if (mapKey.equals("")) return "";

        ArrayList<String> insertionValues = sqlMap.get(mapKey);
        StringBuffer sqlStrBuff = new StringBuffer();
        sqlStrBuff.append(insertionValues.get(0));
        String temp = insertionValues.get(0);

        for (int i = 1; i < insertionValues.size(); i++) {
            sqlStrBuff.append(" " + insertionValues.get(i) + ",");
        }

        sqlStrBuff.deleteCharAt(sqlStrBuff.length() - 1);
        insertionValues.clear();
        insertionValues.add(temp);
        return sqlStrBuff.toString();
    }

    public String resolveInsertSql(String eventType, Set<Map.Entry<String,Object>> set){
        StringBuffer sb = new StringBuffer();
        StringBuffer keys = new StringBuffer();
        sb.append("INSERT INTO dm.dm_v_tr_").append(eventType).append("_mx (*) VALUES ")
                .append("(");
        for(Map.Entry<String,Object> entry: set){
            keys.append(entry.getKey()).append(",");
            if (entry.getValue().toString().equals("")) {
                sb.append("null").append(",");
            } else {
                sb.append('\'');
                sb.append(entry.getValue().toString().replace("\'",""));
                sb.append("\',");
            }
        }
        keys.deleteCharAt(keys.length()-1);
        sb.deleteCharAt(sb.length()-1);
        sb.append(")");

        int target =  sb.indexOf("*");
        sb.deleteCharAt(target);
        sb.insert(target,keys.toString());

        return sb.toString();
    }

    public String resolveSql(RawData rawData){
        // 清洗
        String eventBody = rawData.getEventBody();
        String eventType = rawData.getEventType();
        JSONObject object = JSONObject.parseObject(eventBody);

        switch (eventType){
            case "sa":
                break;
            default:
                break;
        }

        String sql = resolveInsertSql(eventType,object.entrySet());
        putSqlToMap(eventType, sql);
        return sql;
    }
}
