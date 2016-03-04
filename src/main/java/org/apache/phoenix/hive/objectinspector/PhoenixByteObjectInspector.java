/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ByteWritable;

/**
 * @author 주정민
 *
 */
public class PhoenixByteObjectInspector extends AbstractPhoenixObjectInspector<ByteWritable> implements ByteObjectInspector {

	public PhoenixByteObjectInspector() {
		super(TypeInfoFactory.byteTypeInfo);
	}
	
	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Byte((Byte)o);
	}

	@Override
	public byte get(Object o) {
		Byte value = null;
		
		if (o != null) {
			try {
				value = (Byte)o;
			} catch (Exception e) {
				logExceptionMessage(o, "BYTE");
			}
		}
		
		return value;
	}

}
