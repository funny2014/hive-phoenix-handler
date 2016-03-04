/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * @author 주정민
 *
 */
public class PhoenixBooleanObjectInspector extends AbstractPhoenixObjectInspector<BooleanWritable> implements BooleanObjectInspector {

	public PhoenixBooleanObjectInspector() {
		super(TypeInfoFactory.booleanTypeInfo);
	}
	
	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Boolean((Boolean)o);
	}

	@Override
	public boolean get(Object o) {
		Boolean value = null;
		
		if (o != null) {
			try {
				value = (Boolean)o;
			} catch (Exception e) {
				logExceptionMessage(o, "BOOLEAN");
			}
		}
		
		return value;
	}

}
