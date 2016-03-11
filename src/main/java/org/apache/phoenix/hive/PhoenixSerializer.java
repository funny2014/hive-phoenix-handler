/**
 * 
 */
package org.apache.phoenix.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.mapreduce.PhoenixResultWritable;
import org.apache.phoenix.hive.util.PhoenixConnectionUtil;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;
import org.apache.phoenix.util.ColumnInfo;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixSerializer {

	private static final Log LOG = LogFactory.getLog(PhoenixSerializer.class);
	
	public static enum DmlType {
		NONE,
		SELECT,
		INSERT,
		UPDATE,
		DELETE
	}
	
	private int columnCount = 0;
	private PhoenixResultWritable pResultWritable;
	
	public PhoenixSerializer(Configuration config, Properties tbl) throws SerDeException {
		try(Connection conn = PhoenixConnectionUtil.getInputConnection(config, tbl)) {
			List<ColumnInfo> columnMetadata = PhoenixUtil.getColumnInfoList(conn, tbl.getProperty(PhoenixStorageHandlerConstants.PHOENIX_TABLE_NAME));
			
			columnCount = columnMetadata.size();
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("<<<<<<<<<< column-meta : " + columnMetadata + " >>>>>>>>>>");
			}
			
			pResultWritable = new PhoenixResultWritable(config, columnMetadata);
		} catch (SQLException | IOException e) {
			throw new SerDeException(e);
		}
	}

	public Writable serialize(Object values, ObjectInspector objInspector, DmlType dmlType) {
		pResultWritable.clear();
		
		final StructObjectInspector structInspector = (StructObjectInspector)objInspector;
        final List<? extends StructField> fieldList = structInspector.getAllStructFieldRefs();
		
        if (LOG.isTraceEnabled()) {
        	LOG.trace("<<<<<<<<<< fieldList : " + fieldList + " >>>>>>>>>>");
        	LOG.trace("<<<<<<<<<< values(" + values.getClass() + ") : " + values + " >>>>>>>>>>");
        }

        int fieldCount = columnCount;
        if (dmlType == DmlType.UPDATE || dmlType == DmlType.DELETE) {
        	fieldCount++;
        }
        
        for (int i = 0; i < fieldCount; i++) {
        	if (fieldList.size() <= i) {
        		break;
        	}
        	
            StructField structField = fieldList.get(i);
            
            if (LOG.isTraceEnabled()) {
            	LOG.trace("<<<<<<<<<< structField[" + i + "] : " + structField + " >>>>>>>>>>");
            }
            
            if (structField != null) {
                Object fieldValue = structInspector.getStructFieldData(values, structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();
                
                String fieldName = structField.getFieldName();
                
                if (LOG.isTraceEnabled()) {
                	LOG.trace("<<<<<<<<<< " + fieldName + "[" + i + "] : " + fieldValue + ", " + fieldOI + " >>>>>>>>>>");
                }
                
                Object value = null;
                switch (fieldOI.getCategory()) {
	                case PRIMITIVE:
	                    value = ((PrimitiveObjectInspector) fieldOI).getPrimitiveJavaObject(fieldValue);
	                    
	                    if (LOG.isTraceEnabled()) {
	                    	LOG.trace("<<<<<<<<<< " + fieldName + "[" + i + "] : " + value + "(" + value.getClass() + ") >>>>>>>>>>");
	                    }
	                    
	                    if (value instanceof HiveDecimal) {
	                    	value = ((HiveDecimal)value).bigDecimalValue();
	                    } else if (value instanceof HiveChar) {
	                    	value = ((HiveChar)value).getValue().trim();
	                    }
	                    
	                    pResultWritable.add(value);
	                    break;
	                case LIST:
//	                	pResultWritable.add(fieldValue);	// Not support array in insert ... values statement of hive.
	                	break;
	                case STRUCT:
	                	if (dmlType == DmlType.DELETE) {
	                		// When update/delete, First value is struct<transactionid:bigint,bucketid:int,rowid:bigint,primaryKey:binary>>
	                		List<Object> fieldValueList = ((StandardStructObjectInspector)fieldOI).getStructFieldsDataAsList(fieldValue);
	                		
	                		// convert to map from binary of primary key.
	                		@SuppressWarnings("unchecked")
							Map<String, Object> primaryKeyMap = (Map<String, Object>) PhoenixStorageHandlerUtil.toMap(((BytesWritable)fieldValueList.get(3)).getBytes());
	                		for (Object pkValue : primaryKeyMap.values()) {
	                			pResultWritable.add(pkValue);
	                		}
	                		
	                		// In case of Map<String, String>
//	                		@SuppressWarnings("unchecked")
//							Map<String, String> primaryKeyMap = Maps.transformValues((Map<String, Text>) fieldValueList.get(3), new Function<Text, String>() {
//
//								@Override
//								public String apply(Text input) {
//									return input.toString();
//								}
//							});
//	                		
//	                		for (String pkValue : primaryKeyMap.values()) {
//	                			pResultWritable.add(pkValue);
//	                		}
	                	}
	                	
	                	break;
	                default:
	                    new SerDeException("Phoenix Unsupported column type: " + fieldOI.getCategory());
                }
            }
        }
		
		return pResultWritable;
	}
}
