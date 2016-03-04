/**
 * 
 */
package org.apache.phoenix.hive.ppd;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.phoenix.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.hive.ql.index.PredicateAnalyzerFactory;

/**
 * @author 주정민
 *
 */
public class PhoenixPredicateDecomposer {

	private static final Log LOG = LogFactory.getLog(PhoenixPredicateDecomposer.class);

	private List<String> columnNameList;
	private boolean calledPPD;
	
	private List<IndexSearchCondition> searchConditionList;
	
	public PhoenixPredicateDecomposer(List<String> columnNameList) {
		this.columnNameList = columnNameList;
	}
	
	public DecomposedPredicate decomposePredicate(ExprNodeDesc predicate) {
		IndexPredicateAnalyzer analyzer = PredicateAnalyzerFactory.createPredicateAnalyzer(columnNameList, getFieldValidator());
		DecomposedPredicate decomposed = new DecomposedPredicate();

		List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
		decomposed.residualPredicate = (ExprNodeGenericFuncDesc) analyzer.analyzePredicate(predicate, conditions);
		if (!conditions.isEmpty()) {
			decomposed.pushedPredicate = analyzer.translateSearchConditions(conditions);
			try {
				searchConditionList = conditions;
				calledPPD = true;
			} catch (Exception e) {
				LOG.warn("Failed to decompose predicates", e);
				return null;
			}
		}
		
		return decomposed;
	}
	
	public List<IndexSearchCondition> getSearchConditionList() {
		return searchConditionList;
	}

	public boolean isCalledPPD() {
		return calledPPD;
	}
	
	protected IndexPredicateAnalyzer.FieldValidator getFieldValidator() {
		return null;
	}
}
