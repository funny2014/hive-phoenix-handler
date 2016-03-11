/**
 * 
 */
package org.apache.phoenix.hive;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixRowKey extends RecordIdentifier {

//	private Map<String, Object> rowKeyMap = Maps.newHashMap();

	public PhoenixRowKey() {
		
	}
	
//	public void add(String columnName, Object value) {
//		rowKeyMap.put(columnName, value);
//	}
//	
//	public void setRowKeyMap(Map<String, Object> rowKeyMap) {
//		this.rowKeyMap = rowKeyMap;
//	}

//	@Override
//	public void write(DataOutput dataOutput) throws IOException {
//		super.write(dataOutput);
//		
//		try (ObjectOutputStream oos = new ObjectOutputStream((OutputStream) dataOutput)) {
//			oos.writeObject(rowKeyMap);
//			oos.flush();
//		}
//	}
//
//	@SuppressWarnings("unchecked")
//	@Override
//	public void readFields(DataInput dataInput) throws IOException {
//		super.readFields(dataInput);
//		
//		try (ObjectInputStream ois = new ObjectInputStream((InputStream) dataInput)) {
//			rowKeyMap = (Map<String, Object>) ois.readObject();
//		} catch (ClassNotFoundException e) {
//			throw new RuntimeException(e);
//		}
//	}
}
