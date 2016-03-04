/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.AbstractPrimitiveLazyObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;

public abstract class AbstractPhoenixObjectInspector<T extends Writable> extends AbstractPrimitiveLazyObjectInspector<T> {

	private final Log log;
	
	public AbstractPhoenixObjectInspector() {
		super();
		
		log = LogFactory.getLog(getClass());
	}

	protected AbstractPhoenixObjectInspector(PrimitiveTypeInfo typeInfo) {
		super(typeInfo);

		log = LogFactory.getLog(getClass());
	}

	@Override
	public Object getPrimitiveJavaObject(Object o) {
		return o == null ? null : o;
	}

	public void logExceptionMessage(Object value, String dataType) {
		if (log.isDebugEnabled()) {
			log.debug("Data not in the " + dataType + " data type range so converted to null. Given data is :"
					+ value.toString(), new Exception("For debugging purposes"));
		}
	}
}