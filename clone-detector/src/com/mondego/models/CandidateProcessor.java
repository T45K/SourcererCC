package com.mondego.models;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mondego.indexbased.SearchManager;
import com.mondego.utility.Util;

public class CandidateProcessor implements IListener, Runnable {
    private QueryCandidates qc;
    private static final Logger logger = LogManager.getLogger(CandidateProcessor.class);

    public CandidateProcessor(QueryCandidates qc) {
        // TODO Auto-generated constructor stub
        this.qc = qc;
    }

    @Override
    public void run() {
        try {
            // System.out.println( "QCQ size: "+
            // SearchManager.queryCandidatesQueue.size() + Util.debug_thread());
            this.processCandidates();
        } catch (NoSuchElementException e) {
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("EXCEPTION CAUGHT::", e);
            e.printStackTrace();
        }
    }

    /*
     * private void processCandidates() throws InterruptedException,
     * InstantiationException, IllegalAccessException, IllegalArgumentException,
     * InvocationTargetException, NoSuchMethodException, SecurityException { //
     * CandidatePair candidatePair = new CandidatePair(queryBlock, simInfo, //
     * computedThreshold, candidateSize, functionIdCandidate, candidateId)
     * logger.debug("num candidates before: " + (this.qc.candidateUpperIndex -
     * this.qc.candidateLowerIndex));
     * 
     * if (SearchManager.ijaMapping.containsKey(this.qc.queryBlock.fqmn)) {
     * logger.debug("num candidates: " + (this.qc.candidateUpperIndex -
     * this.qc.candidateLowerIndex)); for (int i = this.qc.candidateLowerIndex;
     * i <= this.qc.candidateUpperIndex; i++) { // long startTime =
     * System.nanoTime(); Block candidateBlock =
     * SearchManager.candidatesList.get(i); if (candidateBlock.rowId <
     * this.qc.queryBlock.rowId) { if (candidateBlock.size >=
     * this.qc.queryBlock.computedThreshold && candidateBlock.size <=
     * this.qc.queryBlock.maxCandidateSize) { if
     * (SearchManager.ijaMapping.containsKey(candidateBlock.fqmn)) { if
     * (this.getJaccard(candidateBlock.uniqueChars,
     * this.qc.queryBlock.uniqueChars) > 0.5) { String[] features =
     * this.getLineToWrite(qc.queryBlock, candidateBlock); String line =
     * this.getLineToSend(features); try { //
     * SearchManager.reportCloneQueue.send(new // ClonePair(line));
     * SearchManager.socketWriter.writeToSocket(line); } catch (Exception e) {
     * e.printStackTrace(); } } } } } } //
     * SearchManager.verifyCandidateQueue.send(candidatePair); } }
     */

    private double getJaccard(int sim, int numtokens1, int numtokens2) {
        return sim * 100 / (numtokens1 + numtokens2 - sim);
    }

    private List<String> getLineToWrite(Block queryBlock, Block candiadteBlock) {
        // method1~~method2~~isCLone~~COMP~~NOS~~HVOC~~HEFF~~CREF~~XMET~~LMET~~NOA~~HDIF~~VDEC~~EXCT~~EXCR~~CAST~~NAND~~VREF~~NOPR~~MDN~~NEXP~~LOOP~~NBLTRL~~NCLTRL~~NNLTRL~~NNULLTRL~~NSLTRL
        List<String> features = new ArrayList<String>();
        features.add(queryBlock.getMethodIdentifier());
        features.add(candiadteBlock.getMethodIdentifier());
        String cp = queryBlock.parentId + "," + queryBlock.id + "," + candiadteBlock.parentId + "," + candiadteBlock.id;
        if (SearchManager.clonePairs.contains(cp)) {
            features.add("1");
        } else {
            cp = candiadteBlock.parentId + "," + candiadteBlock.id + "," + queryBlock.parentId + "," + queryBlock.id;
            if (SearchManager.clonePairs.contains(cp)) {
                features.add("1");
            } else {
                features.add("0");
            }
        }
        for(int i = 0; i < queryBlock.metrics.size(); i++){
            features.add(roundThreeDecimal(
                    getPercentageDiff(queryBlock.metrics.get(i), candiadteBlock.metrics.get(i), 10)).toString());
        }
        return features;
    }

    private Double getPercentageDiff(double firstValue, double secondValue, int padding) {
        if (padding > 0) {
            return (Math.abs(firstValue - secondValue) / (padding + Math.max(firstValue + 1, secondValue + 1)));
        }
        return (Math.abs(firstValue - secondValue) / (padding + Math.max(firstValue, secondValue))) * 100;
    }

    private Double roundThreeDecimal(double param) {
        return Double.valueOf(Math.round(param * 1000.0) / 1000.0);
    }

    private String getLineToSend(List<String> lineParams) {
        StringBuilder line = new StringBuilder("");
        String sep = "";
        for (String feature :lineParams) {
            line.append(sep).append(feature);
            sep = "~~";
        }
        return line.toString();
    }

    private void processCandidates() throws InterruptedException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
        // System.out.println("HERE, thread_id: " + Util.debug_thread() +
        // ", query_id "+ queryBlock.getId());
        Map<Long, CandidateSimInfo> simMap = this.qc.simMap;
        Block queryBlock = this.qc.queryBlock;
        logger.debug(
                SearchManager.NODE_PREFIX + ", num candidates: " + simMap.entrySet().size() + ", query: " + queryBlock);
        for (Entry<Long, CandidateSimInfo> entry : simMap.entrySet()) {
            CandidateSimInfo simInfo = entry.getValue();
            Block candidateBlock = simInfo.doc;
            if (candidateBlock.size >= this.qc.queryBlock.minCandidateSize
                    && candidateBlock.size <= this.qc.queryBlock.maxCandidateSize) {
                int minPosibleSimilarity = this.qc.queryBlock.minCandidateActionTokens;
                if (candidateBlock.numActionTokens > this.qc.queryBlock.numActionTokens) {
                    minPosibleSimilarity = candidateBlock.minCandidateActionTokens;
                }
                if (simInfo.totalActionTokenSimilarity >= minPosibleSimilarity) {
                    logger.debug("similarity is: " + simInfo.totalActionTokenSimilarity);
                    String type = "3.2";
                    if (candidateBlock.metriHash.equals(this.qc.queryBlock.metriHash)) {
                        type = "2";
                    } else if (this.getPercentageDiff(candidateBlock.size, this.qc.queryBlock.size, 0) < 11) {
                        type = "3.1";
                    }
                    String line = this.getLineToSend(this.getLineToWrite(qc.queryBlock, candidateBlock));
                    try {
                        if(type.equals("3.1")){
                            logger.debug(type+"#$#"+line);
                            SearchManager.updateClonePairsCount(1);
                            Util.writeToFile(SearchManager.type_3_1_train_Writer, line, true);
                        }else if(type.equals("3.2")){
                            logger.debug(type+"#$#"+line);
                            SearchManager.updateClonePairsCount(1);
                            Util.writeToFile(SearchManager.type_3_2_train_Writer, line, true);
                        }
                        //SearchManager.reportCloneQueue
                        //SearchManager.socketWriter.writeToSocket(type + "#$#" + line);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
