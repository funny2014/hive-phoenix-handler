/**
 * 
 */
package org.apache.phoenix.hive;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.StructObject;

import com.google.common.collect.Lists;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixRow implements StructObject {

	private List<String> columnList;
	private Map<String, Object> resultRowMap;
	
	public PhoenixRow(List<String> columnList) {
		this.columnList = columnList;
	}

	public PhoenixRow setResultRowMap(Map<String, Object> resultRowMap) {
		this.resultRowMap = resultRowMap;
		
		return this;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.StructObject#getField(int)
	 */
	@Override
	public Object getField(int fieldID) {
		return resultRowMap.get(columnList.get(fieldID));
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hive.serde2.StructObject#getFieldsAsList()
	 */
	@Override
	public List<Object> getFieldsAsList() {
		return Lists.newArrayList(resultRowMap.values());
	}


	@Override
	public String toString() {
		return resultRowMap.toString();
	}

}
