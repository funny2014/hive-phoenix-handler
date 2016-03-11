/**
 * 
 */
package org.apache.phoenix.hive.ql.index;

import java.util.List;

import org.apache.phoenix.hive.ql.index.IndexPredicateAnalyzer.FieldValidator;

/**
 * @author JeongMin Ju
 *
 */
public class PredicateAnalyzerFactory {

	public static IndexPredicateAnalyzer createPredicateAnalyzer(List<String> ppdColumnList, FieldValidator fieldValdator) {
		IndexPredicateAnalyzer analyzer = IndexPredicateAnalyzer.createAnalyzer(false);		// =, <, <=, >, >= 포함
		
		for (String columnName : ppdColumnList) {
			analyzer.allowColumnName(columnName);
		}
		
		analyzer.setAcceptsFields(true);
		analyzer.setFieldValidator(fieldValdator);
		
		return analyzer;
	}

}
