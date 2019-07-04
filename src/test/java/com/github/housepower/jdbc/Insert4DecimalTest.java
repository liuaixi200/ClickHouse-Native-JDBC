    
/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.housepower.jdbc;

import com.github.housepower.jdbc.buffer.BuffedReader;
import com.github.housepower.jdbc.buffer.CompressedBuffedReader;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * 功能描述
 *
 * @author liuax01
 * @date 2019/5/16 15:32
 */
public class Insert4DecimalTest {

    @Test
    public void testQuery() throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from book");
        int col = resultSet.getMetaData().getColumnCount();
        while(resultSet.next()){
            for(int i =1;i<=col;i++){
                System.out.println(resultSet.getObject(i));
            }

        }
        System.out.println("ok");
    }

    @Test
    public void testInsert() throws SQLException {
        Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(" insert into book values('a++',12,'2.10','2019-05-16 16:02:02',NULL,NULL,NULL);");

        System.out.println("ok");
    }

    private Connection getConnection(){

        String connectionStr = "jdbc:clickhouse://10.0.73.20:9000/test";
        ClickHouseDriver driver = new ClickHouseDriver();
        String username = "default"; //vsfc_uat
        String password = "A123456a";//Pr75@X&kdN
        Properties properties = new Properties();
        properties.put("user",username);
        properties.put("password",password);
        Connection connection = null;
        try {
            connection = driver.connect(connectionStr,properties);


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
