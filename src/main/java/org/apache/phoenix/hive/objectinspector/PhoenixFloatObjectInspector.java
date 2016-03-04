/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;

/**
 * @author 주정민
 *
 */
public class PhoenixFloatObjectInspector extends AbstractPhoenixObjectInspector<FloatWritable> implements FloatObjectInspector {

	public PhoenixFloatObjectInspector() {
		super(TypeInfoFactory.floatTypeInfo);
	}

	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Float((Float)o);
	}

	@Override
	public float get(Object o) {
		Float value = null;
		
		if (o != null) {
			try {
				value = ((Float)o).floatValue();
			} catch (Exception e) {
				logExceptionMessage(o, "LONG");
			}
		}
		
		return value;
	}

}
