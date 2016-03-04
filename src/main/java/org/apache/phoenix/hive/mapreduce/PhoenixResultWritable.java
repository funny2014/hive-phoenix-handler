/**
 * 
 */
package org.apache.phoenix.hive.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.hive.PhoenixRowKey;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.util.ColumnInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author 주정민
 *
 */
public class PhoenixResultWritable implements Writable, DBWritable, Configurable {

	private static final Log LOG = LogFactory.getLog(PhoenixResultWritable.class);
	
	private List<ColumnInfo> columnMetadataList;
    private List<Object> valueList;	// for output
	private Map<String, Object> rowMap = Maps.newHashMap();  // for input
    
    private int columnCount = -1;

    private Configuration config;
    private boolean isTransactional;
    private Map<String, Object> rowKeyMap = Maps.newLinkedHashMap();
    private List<String> primaryKeyColumnList;
    
    public PhoenixResultWritable() {
    }
    
    public PhoenixResultWritable(Configuration config) throws IOException {
    	setConf(config);
    }
    
    public PhoenixResultWritable(Configuration config, List<ColumnInfo> columnMetadataList) throws IOException {
    	this(config);
    	this.columnMetadataList = columnMetadataList;
    	
    	valueList = Lists.newArrayListWithExpectedSize(columnMetadataList.size());
    }
    
	@Override
	public void write(DataOutput out) throws IOException {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	}
	
	// for write
	public void clear() {
		valueList.clear();
	}
	
	// for write
	public void add(Object value) {
		valueList.add(value);
	}
	
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		ColumnInfo columnInfo = null;
		Object value = null;
		
		try {
			for (int i = 0, limit = columnMetadataList.size(); i < limit; i++) {
				columnInfo = columnMetadataList.get(i);
				
				if (valueList.size() > i) {
					value = valueList.get(i);
				} else {
					value = null;
				}
				
				if (value == null) {
					statement.setNull(i + 1, columnInfo.getSqlType());
				} else {
					statement.setObject(i + 1, value, columnInfo.getSqlType());
				}
			}
		} catch (SQLException|RuntimeException e) {
			LOG.error("<<<<<<<<<< [column-info, value] : " + columnInfo + ", " + value + " >>>>>>>>>>");
			
			throw e;
		}
	}

	public void delete(PreparedStatement statement) throws SQLException {
		ColumnInfo columnInfo = null;
		Object value = null;
		
		try {
			for (int i = 0, limit = primaryKeyColumnList.size(); i < limit; i++) {
				columnInfo = columnMetadataList.get(i);
				
				if (valueList.size() > i) {
					value = valueList.get(i);
				} else {
					value = null;
				}
				
				if (value == null) {
					statement.setNull(i + 1, columnInfo.getSqlType());
				} else {
					statement.setObject(i + 1, value, columnInfo.getSqlType());
				}
			}
		} catch (SQLException|RuntimeException e) {
			LOG.error("<<<<<<<<<< [column-info, value] : " + columnInfo + ", " + value + " >>>>>>>>>>");
			
			throw e;
		}
	}
	
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		if(columnCount == -1) {
            this.columnCount = rsmd.getColumnCount();
        }
  
		rowMap.clear();
		
        for(int i = 0 ; i < columnCount ; i++) {
            Object value = resultSet.getObject(i + 1);
            
            rowMap.put(rsmd.getColumnName(i + 1), value);
        }
        
        // 2016-01-27 주정민 추가 : row__id 컬럼을 추가한다.
        if (isTransactional) {
        	rowKeyMap.clear();
        	
        	for (String pkColumn : primaryKeyColumnList) {
        		rowKeyMap.put(pkColumn, rowMap.get(pkColumn));
        	}
        	
//        	Object[] rowId = new Object[]{PhoenixStorageHandlerConstants.INT_ZERO, PhoenixStorageHandlerConstants.INT_ZERO, rowKeyMap};
//        	
//        	rowMap.put(VirtualColumn.ROWID.getName(), rowId);
        }
	}
	
	public void readPrimaryKey(PhoenixRowKey rowKey) {
		rowKey.setPrimaryKeyMap(rowKeyMap);
//		rowKey.setPrimaryKeyMap(Maps.transformValues(rowKeyMap, new Function<Object, String>() {
//
//			@Override
//			public String apply(Object input) {
//				return input.toString();
//			}
//		}));
	}
	
	public List<ColumnInfo> getColumnMetadataList() {
        return columnMetadataList;
    }

    public void setColumnMetadataList(List<ColumnInfo> columnMetadataList) {
        this.columnMetadataList = columnMetadataList;
    }

	public Map<String, Object> getResultMap() {
		return rowMap;
	}

	public List<Object> getValueList() {
		return valueList;
	}

	@Override
	public void setConf(Configuration conf) {
		config = conf;
		
		isTransactional = PhoenixStorageHandlerUtil.isTransactionalTable(config);
    	
    	if (isTransactional) {
    		primaryKeyColumnList = PhoenixUtil.getPrimaryKeyColumnList(config, config.get(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME));
    	}
	}

	@Override
	public Configuration getConf() {
		return config;
	}
}
