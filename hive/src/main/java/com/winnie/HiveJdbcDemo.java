package com.winnie;

import java.sql.*;

public class HiveJdbcDemo {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection connection = DriverManager.getConnection("jdbc:hive2://testcdh001:10000/default");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from kylin_country");
        while(resultSet.next()){
            System.out.println(resultSet.getString(1));
        }
    }
}
