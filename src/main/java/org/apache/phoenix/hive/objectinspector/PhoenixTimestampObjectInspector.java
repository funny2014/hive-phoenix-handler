/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixTimestampObjectInspector extends AbstractPhoenixObjectInspector<TimestampWritable> implements TimestampObjectInspector {

	public PhoenixTimestampObjectInspector() {
		super(TypeInfoFactory.timestampTypeInfo);
	}

	@Override
	public Timestamp getPrimitiveJavaObject(Object o) {
		return (Timestamp)o;
	}
	
	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Timestamp(((Timestamp)o).getTime());
	}

	@Override
	public TimestampWritable getPrimitiveWritableObject(Object o) {
		TimestampWritable value = null;
		
		if (o != null) {
			try {
				value = new TimestampWritable((Timestamp)o);
			} catch (Exception e) {
				logExceptionMessage(o, "TIMESTAMP");
			}
		}
		
		return value;
	}
}
