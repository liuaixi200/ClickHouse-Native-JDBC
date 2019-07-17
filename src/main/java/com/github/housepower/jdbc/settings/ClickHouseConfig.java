package com.github.housepower.jdbc.settings;

import com.github.housepower.jdbc.misc.Validate;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickHouseConfig {

    private int port;

    private String address;

    private String database;

    private String username;

    private String password;

    private int soTimeout;

    private int connectTimeout;

    private Map<SettingKey, Object> settings;

    private HostInfo[] hostInfos;

    private int goodIndex;

    public static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_]+)");


    private ClickHouseConfig() {
    }

    public ClickHouseConfig(String url, Properties properties) throws SQLException {
        this.settings = parseJDBCUrl(url);
        this.settings.putAll(parseJDBCProperties(properties));

        Object obj;
        this.port = (obj = settings.remove(SettingKey.port)) == null ? 9000 : ((Integer) obj) == -1 ? 9000 : (Integer) obj;
        this.address = (obj = settings.remove(SettingKey.address)) == null ? "127.0.0.1" : String.valueOf(obj);
        this.password = (obj = settings.remove(SettingKey.password)) == null ? "" : String.valueOf(obj);
        this.username = (obj = settings.remove(SettingKey.user)) == null ? "default" : String.valueOf(obj);
        this.database = (obj = settings.remove(SettingKey.database)) == null ? "default" : String.valueOf(obj);
        this.soTimeout = (obj = settings.remove(SettingKey.query_timeout)) == null ? 0 : (Integer) obj;
        this.connectTimeout = (obj = settings.remove(SettingKey.connect_timeout)) == null ? 0 : (Integer) obj;
    }

    public int port() {
        return this.port;
    }

    public String address() {
        return this.address;
    }

    public String database() {
        return this.database;
    }

    public String username() {
        return this.username;
    }

    public String password() {
        return this.password;
    }

    public int queryTimeout() {
        return this.soTimeout;
    }

    public int connectTimeout() {
        return this.connectTimeout;
    }

    public Map<SettingKey, Object> settings() {
        return settings;
    }

    public Map<SettingKey, Object> parseJDBCProperties(Properties properties) {
        Map<SettingKey, Object> settings = new HashMap<SettingKey, Object>();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            for (SettingKey settingKey : SettingKey.values()) {
                String name = String.valueOf(entry.getKey());
                if (settingKey.name().equalsIgnoreCase(name)) {
                    settings.put(settingKey, settingKey.type().deserializeURL(String.valueOf(entry.getValue())));
                }
            }
        }

        return settings;
    }

    private Map<SettingKey, Object> parseJDBCUrl(String jdbcUrl) throws SQLException {
        try {
            // 4  1    10    1 2
            //jdbc:clickhouse://10.0.73.20:9000/vsfc?  //jdbc:clickhouse:10.0.73.20:9000,10.0.73.20:9001/vsfc?
            String hostUrl = jdbcUrl.substring(18);//   //10.0.73.20:9000,10.0.73.20:9001/vsfc?
            //逗号截取,是否有多个数据源
            int endIndex = hostUrl.indexOf("/");
            String hostContent = hostUrl.substring(0,endIndex); //  10.0.73.20:9000,10.0.73.20:9001
            hostInfos = parse2HostInfo(hostContent);
            String parseUrl = "clickhouse://"+hostInfos[0].getAddress()+":"+hostInfos[0].getPort()+hostUrl.substring(endIndex);
            URI uri = new URI(parseUrl);
            Map<SettingKey, Object> settings = new HashMap<SettingKey, Object>();

            String database = uri.getPath();
            if (database != null && !database.isEmpty()) {
                Matcher m = DB_PATH_PATTERN.matcher(database);
                if (m.matches()) {
                    settings.put(SettingKey.database, m.group(1));
                } else {
                    throw new URISyntaxException("wrong database name path: '" + database + "'", jdbcUrl);
                }
            }

            settings.put(SettingKey.port, uri.getPort());
            settings.put(SettingKey.address, uri.getHost());
            settings.putAll(extractQueryParameters(uri.getQuery()));

            return settings;
        } catch (URISyntaxException ex) {
            throw new SQLException(ex.getMessage(), ex);
        }
    }

    private Map<SettingKey, Object> extractQueryParameters(String queryParameters) throws SQLException {
        Map<SettingKey, Object> parameters = new HashMap<SettingKey, Object>();
        StringTokenizer tokenizer = new StringTokenizer(queryParameters == null ? "" : queryParameters, "&");

        while (tokenizer.hasMoreTokens()) {
            String[] queryParameter = tokenizer.nextToken().split("=", 2);
            Validate.isTrue(queryParameter.length == 2,
                "ClickHouse JDBC URL Parameter '" + queryParameters + "' Error, Expected '='.");

            for (SettingKey settingKey : SettingKey.values()) {
                if (settingKey.name().equalsIgnoreCase(queryParameter[0])) {
                    parameters.put(settingKey, settingKey.type().deserializeURL(queryParameter[1]));
                }
            }
        }
        return parameters;
    }

    public void setQueryTimeout(int timeout){
        this.soTimeout = timeout;
    }

    public ClickHouseConfig copy() {
        ClickHouseConfig configure = new ClickHouseConfig();
        configure.port = this.port;
        configure.address = this.address;
        configure.database = this.database;
        configure.username = this.username;
        configure.password = this.password;
        configure.soTimeout = this.soTimeout;
        configure.connectTimeout = this.connectTimeout;
        configure.settings = new HashMap<SettingKey, Object>(this.settings);

        return configure;
    }

    public HostInfo[] getHostInfos() {
        return hostInfos;
    }

    public int getGoodIndex() {
        return goodIndex;
    }

    public void setGoodIndex(int goodIndex) {
        this.goodIndex = goodIndex;
        this.address = hostInfos[goodIndex].getAddress();
        this.port = hostInfos[goodIndex].getPort();
    }

    private HostInfo[] parse2HostInfo(String url){
        String[] strs = url.split(",");
        HostInfo[] hss = new HostInfo[strs.length];
        for(int i =0;i<strs.length;i++){
            String ss = strs[i];
            String[] hs = ss.split(":");
            HostInfo hi = new HostInfo();
            hi.setAddress(hs[0]);
            hi.setPort(Integer.parseInt(hs[1]));
            hss[i] = hi;
        }
        return hss;
    }

    public static class HostInfo{

        private String address;

        private int port;

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }
}
