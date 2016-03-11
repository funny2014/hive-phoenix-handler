/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixLongObjectInspector extends AbstractPhoenixObjectInspector<LongWritable> implements LongObjectInspector {

	public PhoenixLongObjectInspector() {
		super(TypeInfoFactory.longTypeInfo);
	}

	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Long((Long)o);
	}

	@Override
	public long get(Object o) {
		Long value = null;
		
		if (o != null) {
			try {
				value = ((Long)o).longValue();
			} catch (Exception e) {
				logExceptionMessage(o, "LONG");
			}
		}
		
		return value;
	}

}
