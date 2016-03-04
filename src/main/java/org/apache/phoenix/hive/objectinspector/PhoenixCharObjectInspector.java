/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * @author 주정민
 *
 */
public class PhoenixCharObjectInspector extends AbstractPhoenixObjectInspector<HiveCharWritable> implements HiveCharObjectInspector {

	public PhoenixCharObjectInspector() {
			super(TypeInfoFactory.charTypeInfo);
	}
	
	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new String((String)o);
	}

	@Override
	public HiveCharWritable getPrimitiveWritableObject(Object o) {
		return new HiveCharWritable(getPrimitiveJavaObject(o));
	}

	@Override
	public HiveChar getPrimitiveJavaObject(Object o) {
		String value = (String)o;
		return new HiveChar(value, value.length());
	}

}
