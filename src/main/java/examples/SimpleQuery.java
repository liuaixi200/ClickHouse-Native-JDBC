package examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 */
public class SimpleQuery {

  public static void main(String[] args) throws Exception {
    Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
    while(true){
        try {
            Thread.sleep(1000);
            //Connection connection = DriverManager.getConnection("jdbc:clickhouse://10.0.73.20:9000,10.0.73.20:9001/vsfc");
            Connection connection = DriverManager.getConnection("jdbc:clickhouse://10.0.73.20:9000,10.0.73.20:9001/vsfc","default","A123456a");
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from sett_info");
            System.out.println(connection.getMetaData().getURL());
            while (rs.next()) {

                System.out.println(rs.getObject(1) + "\t" + rs.getObject(2));
            }

        } catch (Exception ex){
            ex.printStackTrace();
        }

    }

  }
}
