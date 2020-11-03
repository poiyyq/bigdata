package com.winnie;

import java.sql.*;

public class MysqlDemo {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://cdh001:3306/test", "root", "root");
        PreparedStatement preparedStatement = connection.prepareStatement("select * from test");
        ResultSet resultSet = preparedStatement.executeQuery();
        while(resultSet.next()){
            System.out.println(resultSet.getString(1));
        }
    }
}
