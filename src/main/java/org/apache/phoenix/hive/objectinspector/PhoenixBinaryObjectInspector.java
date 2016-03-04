/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * @author 주정민
 *
 */
public class PhoenixBinaryObjectInspector extends AbstractPhoenixObjectInspector<BytesWritable> implements BinaryObjectInspector {

	public PhoenixBinaryObjectInspector() {
		super(TypeInfoFactory.binaryTypeInfo);
	}
	
	@Override
	public Object copyObject(Object o) {
		byte[] clone = null;
		
		if (o != null) {
			byte[] source = (byte[])o;
			clone = new byte[source.length];
			System.arraycopy(source, 0, clone, 0, source.length);
		}
		
		return clone;
	}

	@Override
	public byte[] getPrimitiveJavaObject(Object o) {
		return (byte[])o;
	}

	@Override
	public BytesWritable getPrimitiveWritableObject(Object o) {
		return new BytesWritable((byte[])o);
	}

}
