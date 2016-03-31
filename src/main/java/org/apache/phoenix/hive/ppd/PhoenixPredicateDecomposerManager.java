/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.ppd;

import java.util.Iterator;
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
	
	// Clear All PredicateDecomposer.
	public static void cleanPredicateDecomposer(String prefixPredicateKey) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("<<<<<<<<<< sessionKey : " + prefixPredicateKey + " >>>>>>>>>>");
		}
		
		for (Iterator<String> iterator = PREDICATE_DECOMPOSER_MAP.keySet().iterator(); iterator.hasNext(); ) {
			String key = iterator.next();
			if (key.startsWith(prefixPredicateKey)) {
				PREDICATE_DECOMPOSER_MAP.remove(key);
			}
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("<<<<<<<<<< predicate-decomposer : " + PREDICATE_DECOMPOSER_MAP + " >>>>>>>>>>");
			LOG.debug("<<<<<<<<<< Predicate Decomposer Clean >>>>>>>>>>");
		}
	}

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
