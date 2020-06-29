package com.mondego.indexbased;

import com.mondego.models.Bag;
import com.mondego.models.ITokensFileProcessor;
import com.mondego.models.TokenFrequency;
import com.mondego.noindex.CloneHelper;
import com.mondego.utility.TokensFileReader;
import com.mondego.utility.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

/**
 * for every project's input file (one file is one project) read all lines for
 * each line create a Bag. for each project create one output file, this file
 * will have all the tokens, in the bag.
 *
 * @author vaibhavsaini
 */
public class WordFrequencyStore implements ITokensFileProcessor {
    private final CloneHelper cloneHelper;
    private Map<String, Long> wordFreq;
    private IndexWriter wfmIndexWriter = null;
    private DocumentMaker wfmIndexer = null;
    private static final Logger logger = LogManager.getLogger(WordFrequencyStore.class);

    public WordFrequencyStore() {
        this.wordFreq = new TreeMap<>();
        this.cloneHelper = new CloneHelper();
    }

    public static void main(String[] args) throws IOException, ParseException {
        WordFrequencyStore wfs = new WordFrequencyStore();
        wfs.populateLocalWordFreqMap();
    }

    /**
     * Reads the input file and writes the partial word frequency maps to .wfm
     * files.
     */
    private void readTokensFile(File file) throws IOException, ParseException {
        TokensFileReader tfr = new TokensFileReader(SearchManager.NODE_PREFIX, file, SearchManager.max_tokens, this);
        tfr.read();
    }

    public void processLine(String line) throws ParseException {

        Bag bag = cloneHelper.deserialise(line);

        if (null != bag && bag.getSize() > SearchManager.min_tokens && bag.getSize() < SearchManager.max_tokens) {
            populateWordFreqMap(bag);
        } else {
            if (null == bag) {
                logger.debug("empty block, ignoring");
            } else {
                logger.debug("not adding tokens of line to WFM, REASON: " + bag.getFunctionId() + ", " + bag.getId()
                    + ", size: " + bag.getSize() + " (max tokens is " + SearchManager.max_tokens + ")");
            }
        }
    }

    private void populateWordFreqMap(Bag bag) {
        for (TokenFrequency tf : bag) {
            String tokenStr = tf.getToken().getValue();
            if (this.wordFreq.containsKey(tokenStr)) {
                long value = this.wordFreq.get(tokenStr) + tf.getFrequency();
                this.wordFreq.put(tokenStr, value);
            } else {
                this.wordFreq.put(tokenStr, (long) tf.getFrequency());
            }
        }
        // if map size if more than 8 Million flush it.
        if (this.wordFreq.size() > 8000000) {
            // write it in a file. it is a treeMap, so it is already sorted by
            // keys (alphbatically)
            flushToIndex();

            // Util.writeMapToFile(SearchManager.WFM_DIR_PATH + "/wordFreqMap_"
            // + TermSorter.wfm_file_count + ".wfm",
            // TermSorter.wordFreq);

            // reinit the map
            this.wordFreq = new TreeMap<>();
        }
    }

    public void populateLocalWordFreqMap() throws IOException, ParseException {

        File queryDir = new File(SearchManager.QUERY_DIR_PATH);
        if (queryDir.isDirectory()) {
            logger.info("Directory: " + queryDir.getAbsolutePath());

            this.prepareIndex();

            for (File inputFile : Objects.requireNonNull(queryDir.listFiles())) {

                this.readTokensFile(inputFile);

            }

            // write the last map to the index
            flushToIndex();
            // Util.writeMapToFile(SearchManager.WFM_DIR_PATH + "/wordFreqMap_"
            // + TermSorter.wfm_file_count + ".wfm",
            // TermSorter.wordFreq);
            wordFreq = null; // we don't need it, let GC get it.
            // shutdown
            shutdown();
        } else {
            logger.error("File: " + queryDir.getName() + " is not a directory. Exiting now");
            System.exit(1);
        }
    }

    public void prepareIndex() {
        File globalWFMDIr = new File(SearchManager.ROOT_DIR + "gtpmindex");
        if (!globalWFMDIr.exists()) {
            Util.createDirs(SearchManager.ROOT_DIR + "gtpmindex");
        }
        KeywordAnalyzer keywordAnalyzer = new KeywordAnalyzer();
        IndexWriterConfig wfmIndexWriterConfig = new IndexWriterConfig(Version.LUCENE_46, keywordAnalyzer);
        wfmIndexWriterConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
        wfmIndexWriterConfig.setRAMBufferSizeMB(1024);

        logger.info("PREPARE INDEX");
        try {
            wfmIndexWriter = new IndexWriter(FSDirectory.open(new File(SearchManager.ROOT_DIR + "gtpmindex")), wfmIndexWriterConfig);
            wfmIndexWriter.commit();
            wfmIndexer = new DocumentMaker(wfmIndexWriter);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void flushToIndex() {
        logger.info("*** FLUSHING WFM TO INDEX *** " + this.wordFreq.size());
        long start = System.currentTimeMillis();
        final CodeSearcher wfmSearcher = new CodeSearcher(SearchManager.ROOT_DIR + "gtpmindex", "key");
        int count = 0;
        for (Entry<String, Long> entry : this.wordFreq.entrySet()) {

            long oldfreq = wfmSearcher.getFrequency(entry.getKey());
            if (oldfreq < 0)
                oldfreq = 0;

            wfmIndexer.indexWFMEntry(entry.getKey(), oldfreq + entry.getValue());
            if (++count % 1000000 == 0)
                logger.info("...flushed " + count);
        }
        wfmSearcher.close();
        try {
            this.wfmIndexWriter.forceMerge(1);
            this.wfmIndexWriter.commit();
        } catch (Exception e) {
            logger.error(SearchManager.NODE_PREFIX + ", exception on commit", e);
            e.printStackTrace();
        }
        long elapsed = System.currentTimeMillis() - start;
        logger.info("*** FLUSHING END *** " + count + " in " + elapsed / 1000 + "s");
    }

    private void shutdown() {
        logger.debug("Shutdown");
        try {
            wfmIndexWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
