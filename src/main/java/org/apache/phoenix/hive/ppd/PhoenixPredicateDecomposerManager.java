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
 * @author JeongMin Ju
 *
 */
public class PhoenixPredicateDecomposerManager {

	private static final Log LOG = LogFactory.getLog(PhoenixPredicateDecomposerManager.class);
	
	// In case of without where clause, PhoenixPredicateDecomposer is not created because it's not called method of StorageHandler.decomposePredicate.
	// Therefore Self-Join is likely to wrong InputFormat.getSplits.
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
