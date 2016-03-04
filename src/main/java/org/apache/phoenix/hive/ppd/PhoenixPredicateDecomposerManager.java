/**
 * 
 */
package org.apache.phoenix.hive.ppd;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author 주정민
 *
 */
public class PhoenixPredicateDecomposerManager {

	private static final Log LOG = LogFactory.getLog(PhoenixPredicateDecomposerManager.class);
	
	// Where 조건절이 없는 경우 StorageHandler.decomposePredicate 메서드가 호출되지 않아 PhoenixPredicateDecomposer가 생성되지 않는다. 
	// 때문에 Self-Join인 경우 InputFormat.getSplits 에서 잘못된 처리가 발생될 소지가 있다. 
	private static final Map<String, List<PhoenixPredicateDecomposer>> PREDICATE_DECOMPOSER_MAP = Maps.newConcurrentMap();
	
	public static PhoenixPredicateDecomposer createPredicateDecomposer(String predicateKey, List<String> columnNameList) {
		List<PhoenixPredicateDecomposer> predicateDecomposerList = PREDICATE_DECOMPOSER_MAP.get(predicateKey);
		if (predicateDecomposerList == null) {
			predicateDecomposerList = Lists.newArrayList();
			PREDICATE_DECOMPOSER_MAP.put(predicateKey, predicateDecomposerList);
		}
		
		PhoenixPredicateDecomposer predicateDecomposer = new PhoenixPredicateDecomposer(columnNameList);
		predicateDecomposerList.add(predicateDecomposer);
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("<<<<<<<<<< predicate-decomposer : " + PREDICATE_DECOMPOSER_MAP + " >>>>>>>>>>");
			LOG.debug("<<<<<<<<<< predicate-decomposer[" + predicateKey + "] : " + predicateDecomposer + " >>>>>>>>>>");
		}
		
		return predicateDecomposer;
	}
	
	public static PhoenixPredicateDecomposer getPredicateDecomposer(String predicateKey) {
		List<PhoenixPredicateDecomposer> predicateDecomposerList = PREDICATE_DECOMPOSER_MAP.get(predicateKey);
		
		PhoenixPredicateDecomposer predicateDecomposer = null;
		if (predicateDecomposerList != null && predicateDecomposerList.size() > 0) {
			predicateDecomposer = predicateDecomposerList.remove(0);
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("<<<<<<<<<< predicate-decomposer : " + PREDICATE_DECOMPOSER_MAP + " >>>>>>>>>>");
			LOG.debug("<<<<<<<<<< predicate-decomposer[" + predicateKey + "] : " + predicateDecomposer + " >>>>>>>>>>");
		}
		
		return predicateDecomposer;
	}
	
	private PhoenixPredicateDecomposerManager() {
	}

}
