/**
 * 
 */
package org.apache.phoenix.hive.objectinspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

/**
 * @author JeongMin Ju
 *
 */
public class PhoenixStringObjectInspector extends AbstractPhoenixObjectInspector<Text> implements StringObjectInspector {

	private boolean escaped;
	private byte escapeChar;

	public PhoenixStringObjectInspector(boolean escaped, byte escapeChar) {
		super(TypeInfoFactory.stringTypeInfo);
		this.escaped = escaped;
		this.escapeChar = escapeChar;
	}

	@Override
	public Object copyObject(Object o) {
		return o == null ? null : new String((String)o);
	}

	@Override
	public String getPrimitiveJavaObject(Object o) {
		return (String)o;
	}

	@Override
	public Text getPrimitiveWritableObject(Object o) {
		Text value = null;
		
		if (o != null) {
			try {
				value = new Text((String)o);
			} catch (Exception e) {
				logExceptionMessage(o, "STRING");
			}
		}
		
		return value;
	}

	public boolean isEscaped() {
		return escaped;
	}

	public byte getEscapeChar() {
		return escapeChar;
	}

}
