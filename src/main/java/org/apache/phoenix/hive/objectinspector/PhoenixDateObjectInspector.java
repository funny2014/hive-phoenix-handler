/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import java.sql.Date;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixDateObjectInspector extends AbstractPhoenixObjectInspector<DateWritable> implements DateObjectInspector {

	public PhoenixDateObjectInspector() {
		super(TypeInfoFactory.dateTypeInfo);
	}

	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Date(((Date)o).getTime());
	}

	@Override
	public DateWritable getPrimitiveWritableObject(Object o) {
		DateWritable value = null;
		
		if (o != null) {
			try {
				value = new DateWritable((Date)o);
			} catch (Exception e) {
				logExceptionMessage(o, "DATE");
				value = new DateWritable();
			}
		}
		
		return value;
	}

	@Override
	public Date getPrimitiveJavaObject(Object o) {
		return (Date)o;
	}

}
