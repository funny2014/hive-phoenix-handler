/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixDecimalObjectInspector extends AbstractPhoenixObjectInspector<HiveDecimalWritable> implements HiveDecimalObjectInspector {

	public PhoenixDecimalObjectInspector() {
		super(TypeInfoFactory.decimalTypeInfo);
	}

	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new Decimal((Decimal)o);
	}

	@Override
	public HiveDecimal getPrimitiveJavaObject(Object o) {
		return HiveDecimal.create((BigDecimal)o);
	}

	@Override
	public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
		HiveDecimalWritable value = null;
		
		if (o != null) {
			try {
				value = new HiveDecimalWritable((HiveDecimalWritable)o);
			} catch (Exception e) {
				logExceptionMessage(o, "DECIMAL");
			}
		}
		
		return value;
		
//		return super.getPrimitiveWritableObject(o);
	}

}
