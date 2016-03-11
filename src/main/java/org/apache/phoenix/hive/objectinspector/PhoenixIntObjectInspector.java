/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixIntObjectInspector extends AbstractPhoenixObjectInspector<IntWritable> implements IntObjectInspector {

	public PhoenixIntObjectInspector() {
		super(TypeInfoFactory.intTypeInfo);
	}
	
	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Integer((Integer)o);
	}

	@Override
	public int get(Object o) {
		Integer value = null;
		
		if (o != null) {
			try {
				value = ((Integer)o).intValue();
			} catch (Exception e) {
				logExceptionMessage(o, "INT");
			}
		}
		
		return value;
	}

}
