package org.apache.lucene.index;

import com.google.common.base.Splitter;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class SegmentMergeTool {

    private final static Logger logger = LoggerFactory.getLogger(SegmentMergeTool.class.getName());

    static class Config {
        String segmentPath;
        List<Integer> deleteSegs;
        String searchTerm;
        List<Integer> mergeSegs;
    }

    static final Splitter COMMA_SPLITTER = Splitter.on(",");

    public static void main (String[] args) throws IOException {

        // TODO : We should skip last argument. its a path
        List<String> argL = Arrays.asList(args);
        if (argL.size() % 2 != 1) {
            logger.error("Each argument needs one value at least");
            return;
        }
        String segmentPath = "./";

        Config cfg = new Config();

        boolean parseOne;
        for (int i = 0; i < argL.size(); i+= parseOne ? 1: 2) {
            parseOne = false;
            switch (argL.get(i)) {
                case "--delete":
                    for (String s : COMMA_SPLITTER.split((argL.get(i+1)))) {
                        if (cfg.deleteSegs == null) {
                            cfg.deleteSegs = new ArrayList<>();
                        }
                        cfg.deleteSegs.add(Integer.parseInt(s));
                    }
                    logger.info("Delete enabled for : {}", Arrays.toString(cfg.deleteSegs.toArray()));
                case "--search":
                    cfg.searchTerm = argL.get(i+1);
                    break;
                case "--merge":
                    for (String s : COMMA_SPLITTER.split((argL.get(i+1)))) {
                        if (cfg.mergeSegs == null) {
                            cfg.mergeSegs = new ArrayList<>();
                        }
                        cfg.mergeSegs.add(Integer.parseInt(s));
                    }
                    logger.info("Merge enabled for : {}", Arrays.toString(cfg.mergeSegs.toArray()));

                    break;
                default:
                    // plus one is always a path
                    segmentPath =  argL.get(i);
                    parseOne = true;
            }
        }
        logger.info("Set path to = {}", segmentPath);
        Directory dir = FSDirectory.open(Paths.get(segmentPath));

        DirectoryReader dreader = DirectoryReader.open(dir);

        IndexCommit ic = dreader.getIndexCommit();
        logger.info("segment count = {}", ic.getSegmentCount());
        logger.info("files= {}", ic.getFileNames());
        logger.info("generation = {}", ic.getGeneration());
        logger.info("segment file name = {}", ic.getSegmentsFileName());
        logger.info("isDeleted?? : {}", ic.isDeleted());
        for (Map.Entry<String, String> entry: ic.getUserData().entrySet()) {
            logger.info("{} {}", entry.getKey(), entry.getValue());
        }

        // segment info
        String files[]  = dir.listAll();
        String lastSegmentFile = SegmentInfos.getLastCommitSegmentsFileName(files);
        logger.info("LastSegment File={}", lastSegmentFile);
        SegmentInfos sis = SegmentInfos.readCommit(dir, lastSegmentFile);
        int numSegments = sis.asList().size();
        logger.info("num of segments = {}", numSegments);
        for (int i = 0; i < numSegments; i++) {
            final SegmentCommitInfo info = sis.info(i);
            logger.info("maxDoc={}", info.info.maxDoc());
            SegmentReader reader = null;
            Sort indexSort = info.info.getIndexSort();
            if (indexSort !=null) {
                logger.info("indexSort {}", indexSort);
            }

            logger.info("{} Size (MB): {}", i, info.sizeInBytes()/(1024. * 1024.));
            try {
                //final Codec codec = info.info.getCodec();
                //logger.info("codec={}", codec);

                logger.info("name={}", info.info.name);

                /*
                Map<String, String> diag = info.info.getDiagnostics();
                for (Map.Entry<String, String> e: diag.entrySet()) {
                    logger.info("{} -> {}", e.getKey(), e.getValue());
                }
                */

                if (!info.hasDeletions()) {
                    logger.info("No deletion");
                } else {
                    logger.info("Deletion: {}", info.getDelCount());
                }
            } catch (Throwable t) {

            }

        }


        if (cfg.deleteSegs != null) {
            logger.info("Invoking deletes, specified {}", Arrays.toString(cfg.deleteSegs.toArray()));
            for (int segidx : cfg.deleteSegs) {
                SegmentCommitInfo sci = sis.info(segidx);
                logger.info("Removing: {}", sci.info.files());
                sis.remove(segidx);
            }
            // Write out metadata.
            sis.prepareCommit(dir);
            sis.finishCommit(dir);
        }



        if (cfg.mergeSegs != null) {
            logger.info("Going for merge");

            // merge
            SegmentCommitInfo si1 = sis.info(1);
            SegmentCommitInfo si2 = sis.info(2);
            //SegmentCommitInfo si3 = sis.info(3);
            //SegmentCommitInfo si4 = sis.info(4);
            IOContext context = new IOContext(
                    new MergeInfo(-1, -1, false, -1));
            SegmentReader r1 = new SegmentReader(si1, context);
            SegmentReader r2 = new SegmentReader(si2, context);
            //SegmentReader r3 = new SegmentReader(si3, context);
            //SegmentReader r4 = new SegmentReader(si4, context);

            final Codec codec = Codec.getDefault();
            final SegmentInfo si = new SegmentInfo(
                    dir,
                    Version.LATEST,
                    "_pei5",
                    -1,
                    false,
                    codec,
                    Collections.emptyMap(),
                    StringHelper.randomId(),
                    new HashMap<>(),
                    null
            );
            TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);

            SegmentMerger merger = new SegmentMerger(
                    //Arrays.asList(r1, r2, r3, r4),
                    Arrays.asList(r1, r2),
                    si,
                    InfoStream.getDefault(),
                    trackingDir,
                    new FieldInfos.FieldNumbers(),
                    context

            );
            MergeState ms = merger.merge();
            logger.info("merge files:{}", trackingDir.getCreatedFiles());
            SegmentCommitInfo infoPerCommit = new SegmentCommitInfo(si, 0, -1L, -1L, -1L);
            si.setFiles(new HashSet<>(trackingDir.getCreatedFiles()));
            trackingDir.clearCreatedFiles();

            codec.segmentInfoFormat().write(trackingDir, si, context);
            logger.info("seg info files:{}", trackingDir.getCreatedFiles());
            si.addFiles(trackingDir.getCreatedFiles());

            sis.add(infoPerCommit);

        }

        if(cfg.searchTerm != null) {

            // Search
            IndexSearcher searcher = new IndexSearcher(dreader);

            BooleanQuery query = new BooleanQuery.Builder()
                    .add(new TermQuery(new Term("name", cfg.searchTerm)), BooleanClause.Occur.MUST)
                    .build();
            TopDocs topDocs = searcher.search(query, 1);
            logger.info("Total hits: {}", topDocs.totalHits);
            logger.info("scoreDocs:{}", topDocs.scoreDocs.length);
        }

    }
}
