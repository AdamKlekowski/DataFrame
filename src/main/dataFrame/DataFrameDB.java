package main.dataFrame;

import main.GraphicalUserInterface.controllers.MainController;
import main.value.*;

import java.sql.*;

public class DataFrameDB {
    private static Connection conn;

    public static void connect(String[] columnNames, String[] columnTypes) throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        MainController.df = new DataFrame(columnNames, columnTypes);

        try {
            String path = "jdbc:sqlite:DataFrame.db";
            conn = DriverManager.getConnection(path);
            System.out.println("Connection to SQLite has been established.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }


        Value[] newRaw = new Value[MainController.df.columns.size()];
        String[] dbRaw = new String[MainController.df.columns.size()];

        String sql = "SELECT * FROM groupBy;";
        PreparedStatement pstmt  = conn.prepareStatement(sql);
        ResultSet rs  = pstmt.executeQuery();

        while (rs.next()) {
            dbRaw[0] = rs.getString("id");
            dbRaw[1] = rs.getString("date");
            dbRaw[2] = rs.getString("total");
            dbRaw[3] = rs.getString("val");

            for (int i=0; i<dbRaw.length; i++) {
                Class c = Class.forName("main.value." + columnTypes[i]);
                switch (columnTypes[i]) {
                    case "StringValue":
                        newRaw[i] = ((StringValue) c.newInstance()).create(dbRaw[i]);
                        break;
                    case "DoubleValue":
                        newRaw[i] = ((DoubleValue) c.newInstance()).create(dbRaw[i]);
                        break;
                    case "IntegerValue":
                        newRaw[i] = ((IntegerValue) c.newInstance()).create(dbRaw[i]);
                        break;
                    case "DataTimeValue":
                        newRaw[i] = ((DataTimeValue) c.newInstance()).create(dbRaw[i]);
                        break;
                }
            }
            MainController.df.add(newRaw);
        }
    }
}