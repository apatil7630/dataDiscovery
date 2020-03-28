package dataDiscovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SensitiveDataDiscoveryTest {

	public static void main(String[] args) {

		performMetaDataMining();
//		performPatternMatching();
	}
	
	public static void performMetaDataMining() {
		
		List<String> userSelColumns = new ArrayList<>();
		userSelColumns.add("name");
		userSelColumns.add("address");
		
		SensitiveDataDiscoveryService service = new SensitiveDataDiscoveryService();
		
		Map<String, List<String>> sensitiveDataMap = service.performMetaDataMining(userSelColumns);
		sensitiveDataMap.forEach((k,v) -> System.out.println(k +v));		
	}
	
	public static void performPatternMatching(){
		
		List<String> userSelFields = new ArrayList<>();
		userSelFields.add("phone");
		
		SensitiveDataDiscoveryService service = new SensitiveDataDiscoveryService();
		
		Map<String, List<String>> sensitiveDataMap = service.performPatternMatching(userSelFields);
		sensitiveDataMap.forEach((k,v) -> System.out.println(k +v));		
	}
	
	public static Map<String, List<String>> getTestMetaDataMap(){
		
		Map<String, List<String>> metaDataMap = new HashMap<String, List<String>>();
		
		List<String> table1Columns = new ArrayList<String>();
		table1Columns.add("fname");
		table1Columns.add("address");
		table1Columns.add("salary");
		metaDataMap.put("table1", table1Columns);
		  
		List<String> table2Columns = new ArrayList<String>();
		table2Columns.add("first_name");
		table2Columns.add("date_of_birth");
		table2Columns.add("description");
		metaDataMap.putIfAbsent("table2", table2Columns);
		
		List<String> table3Columns = new ArrayList<String>();
		table3Columns.add("address");
		table3Columns.add("mail_id");
		table3Columns.add("cc_no");
		table3Columns.add("exp_date");
		metaDataMap.putIfAbsent("table3", table3Columns);
		
		return metaDataMap;
	}
}
