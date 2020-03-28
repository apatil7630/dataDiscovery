package dataDiscovery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractDataSource {

	/**
	 * The sub class of this class has to pass the arguments in the below order
	 * 
	 * First parameter should be drivername
	 * Second parameter should be URL
	 * Third parameter should be user name
	 * Fourth parameter should be password
	 * 
	 * @param connData
	 * @return
	 */
	protected Connection getConnection() {

		Connection conn = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");

			// STEP 2: Open a connection
			conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "root");

		} catch (SQLException | ClassNotFoundException e) {

			System.out.println(e.getMessage());
		}
		return conn;
	}
	
	protected void closeResources(Connection conn, ResultSet set, Statement st) {
		
			try {
				if(conn!=null)conn.close();
				if(set!=null)set.close();
				if(st!=null)st.close();
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
}
