package Discovery;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Fetches meta data(like table names and column names from database
 * 
 * @author Kiran
 *
 */
public class SensitiveDataDiscoveryDao extends AbstractDataSource {

	public Map<String, List<String>> fetchMetaData(boolean isOnlyTableDataRequired) {

		PreparedStatement pstmt = null;
		Connection conn = null;
		ResultSet rs = null;
		Map<String, List<String>> metaDataMap = null;

		try {
			conn = super.getConnection();

			DatabaseMetaData dbmd = conn.getMetaData();
			String table[] = { "TABLE" };
			rs = dbmd.getTables(null, null, null, table);

			metaDataMap = new HashMap<>();

			while (rs.next()) {

				// get each table
				String tableName = rs.getString("TABLE_NAME");

				if(!isOnlyTableDataRequired) {

					// fetch columns for each table
					List<String> colList = fetchColumnNames(tableName, dbmd);
	
					// map the columns for respective table
					metaDataMap.putIfAbsent(tableName, colList);
				
				}else{
					metaDataMap.putIfAbsent(tableName, null);
				}
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());

		} finally {
			super.closeResources(conn, rs, pstmt);
		}
		return metaDataMap;
	}

	private List<String> fetchColumnNames(String tableName, DatabaseMetaData dbmd) {

		List<String> colList = new CopyOnWriteArrayList<>();

		try {
			// get columns for each table
			ResultSet resultSet = dbmd.getColumns(null, null, tableName, null);

			// iterate the column names and store in a list
			while (resultSet.next()) {
				colList.add(resultSet.getString("COLUMN_NAME"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return colList;
	}

	/**
	 * 
	 * @param userSelFields
	 * @param level - represents data/column level regular expression
	 * @return List of regular expressions
	 */
	public Map<String, String> getRegExpsFromDB(List<String> userSelFields, char level) {

		// StringBuilder query = new StringBuilder("Select * FROM reg_exprn_t
		// WHERE field_name IN ('name','address')");
		StringBuilder query = new StringBuilder("Select * FROM reg_exprn_t WHERE flag='" + level + "'");
		Statement st = null;
		ResultSet rs = null;

		 Map<String, String> regExMap = new HashMap<>();
		// userSelFields.forEach(ele ->
		// query.append("'").append(ele).append("'").append(","));
		// String actualQuery = query.substring(0,
		// query.lastIndexOf(",")).concat(")");
		// System.out.println(actualQuery);

		try {
			st = super.getConnection().createStatement();
			rs = st.executeQuery(query.toString());

			while (rs.next()) {
				 regExMap.put(rs.getString("field_name"), rs.getString("reg_ex"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return regExMap;
	}

	/**
	 * returns all the rows as a single list
	 * 
	 * @param table
	 * @return
	 */
	public List getDataForPatternMatching(String table) {

		StringBuilder query = new StringBuilder("Select * FROM ").append(table);
		
		//since table will have different types of data, store the data in heterogeneous collection
		List dataList = new ArrayList<>();

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		Map<String, String> columnMap = new HashMap<>();

		try {
			conn = super.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query.toString());
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();

			// keep each column name and its type in a map
			for (int i = 1; i <= columnCount; i++) {
				columnMap.put(rsmd.getColumnName(i), rsmd.getColumnTypeName(i));
			}

			while (rs.next()) {

				// for each column get the name and type
				for (Map.Entry<String, String> entry : columnMap.entrySet()) {

					//change the below data types according to DB2 
					if (entry.getValue().equalsIgnoreCase("INT"))
						dataList.add(rs.getInt(entry.getKey()));
					
					else if (entry.getValue().equalsIgnoreCase("VARCHAR"))
						dataList.add(rs.getString(entry.getKey()));
					
					else if (entry.getValue().equalsIgnoreCase("DATETIME"))
						dataList.add(rs.getString(entry.getKey()));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			super.closeResources(null, rs, stmt);
		}
		return dataList;
	}

	public static void main(String[] args) {
		SensitiveDataDiscoveryDao dao = new SensitiveDataDiscoveryDao();
		List dataList = dao.getDataForPatternMatching("reg_exprn_t");
		System.out.println(dataList);
	}
}
