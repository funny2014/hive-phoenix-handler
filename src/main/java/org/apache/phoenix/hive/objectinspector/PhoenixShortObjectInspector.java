/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ShortWritable;

/**
 * @author 주정민
 *
 */
public class PhoenixShortObjectInspector extends AbstractPhoenixObjectInspector<ShortWritable> implements ShortObjectInspector {

	public PhoenixShortObjectInspector() {
		super(TypeInfoFactory.shortTypeInfo);
	}
	
	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Short((Short)o);
	}

	@Override
	public short get(Object o) {
		Short value = null;
		
		if (o != null) {
			try {
				value = ((Short)o).shortValue();
			} catch (Exception e) {
				logExceptionMessage(o, "SHORT");
			}
		}
		
		return value;
	}

}
