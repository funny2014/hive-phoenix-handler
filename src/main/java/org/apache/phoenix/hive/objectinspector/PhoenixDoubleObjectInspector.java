/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.DoubleWritable;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixDoubleObjectInspector extends AbstractPhoenixObjectInspector<DoubleWritable> implements DoubleObjectInspector {

	public PhoenixDoubleObjectInspector() {
		super(TypeInfoFactory.doubleTypeInfo);
	}

	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Double((Double)o);
	}

	@Override
	public double get(Object o) {
		Double value = null;
		
		if (o != null) {
			try {
				value = ((Double)o).doubleValue();
			} catch (Exception e) {
				logExceptionMessage(o, "LONG");
			}
		}
		
		return value;
	}

}
