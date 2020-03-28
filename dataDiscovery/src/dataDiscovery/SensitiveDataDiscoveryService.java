package dataDiscovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SensitiveDataDiscoveryService {

	private static SensitiveDataDiscoveryDao dao = new SensitiveDataDiscoveryDao();

	public Map<String, List<String>> performMetaDataMining(List<String> userSelFields) {

		Map<String, List<String>> metaDataMap = dao.fetchMetaData(false);
		Map<String, List<String>> sensitiveDataMap = findSensitiveColumns(metaDataMap, userSelFields);

		return sensitiveDataMap;
		// return null;
	}

	private Map<String, List<String>> findSensitiveColumns(
										Map<String, List<String>> metaDataMap,
												List<String> userSelFields) {

		Map<String, List<String>> sensitiveDataMap = new HashMap<>();

		// get regular exp for column level('C') from db
		Map<String, String> dbRegExpMap = dao.getRegExpsFromDB(userSelFields, 'C');

		//for each table, get the column list
		metaDataMap.forEach((tableName, columnList) -> {

			List<String> senColumnList = new ArrayList<>();

			columnList.forEach(eachCol -> {

				//validate whether any of the column matches with 5 sensitive data elements.
				//if matches store in a list
				if (dbRegExpMap.containsKey("name") && Pattern.matches(dbRegExpMap.get("name"), eachCol)) {
					senColumnList.add(eachCol);
				}
				if (dbRegExpMap.containsKey("address") && Pattern.matches(dbRegExpMap.get("address"), eachCol)) {
					senColumnList.add(eachCol);
				}
				if (dbRegExpMap.containsKey("dob") && Pattern.matches(dbRegExpMap.get("dob"), eachCol)) {
					senColumnList.add(eachCol);
				}
				if (dbRegExpMap.containsKey("ccno") && Pattern.matches(dbRegExpMap.get("ccno"), eachCol)) {
					senColumnList.add(eachCol);
				}
				if (dbRegExpMap.containsKey("expdate") && Pattern.matches(dbRegExpMap.get("expdate"), eachCol)) {
					senColumnList.add(eachCol);
				}
			});
			if (senColumnList != null && !senColumnList.isEmpty()) {
				sensitiveDataMap.putIfAbsent(tableName, senColumnList);
			}
		});
		return sensitiveDataMap;
	}

	// apply pattern matching
	public Map<String, List<String>> performPatternMatching(List<String> userSelFields) {

		List<?> dataList = new ArrayList<>();
		Map<String, List<String>> finalMap = new HashMap<>();

		boolean isOnlyTableDataRequired = true;

		// get only table information(but not column info)
		Map<String, List<String>> metaDataMap = dao.fetchMetaData(isOnlyTableDataRequired);

		// for each table, get the data and put in list
		metaDataMap.forEach((table, value) -> {
			dataList.addAll(dao.getDataForPatternMatching(table));
		});
		
		// get regular exprn from db
		Map<String, String> dbRegExpMap = dao.getRegExpsFromDB(userSelFields, 'D');

		// for each table data list
		dataList.forEach(eachEle -> {

			if (dbRegExpMap.containsKey("phone") && eachEle != null &&
					Pattern.matches(dbRegExpMap.get("phone"), eachEle.toString())) {

				if (finalMap.containsKey("phone")) {

					List<String> nameList = finalMap.get("phone");
					putSenDataListInFinalMap(nameList, "phone", eachEle.toString(), finalMap);

				} else {
					List<String> nameList = new ArrayList<>();
					putSenDataListInFinalMap(nameList, "phone", eachEle.toString(), finalMap);
				}
			}
			if (dbRegExpMap.containsKey("name") && eachEle != null &&
					Pattern.matches(dbRegExpMap.get("name"), eachEle.toString())) {

				if (finalMap.containsKey("name")) {

					List<String> nameList = finalMap.get("name");
					putSenDataListInFinalMap(nameList, "name", eachEle.toString(), finalMap);

				} else {
					List<String> nameList = new ArrayList<>();
					putSenDataListInFinalMap(nameList, "name", eachEle.toString(), finalMap);
				}
			}
		});
		return finalMap;
	}//end pattern matching method
	
	private void putSenDataListInFinalMap(List<String> nameList, String userSelField, String eachEle,
			Map<String, List<String>> finalMap){
	
		nameList.add(eachEle);
		finalMap.put(userSelField, nameList);
	}
}
