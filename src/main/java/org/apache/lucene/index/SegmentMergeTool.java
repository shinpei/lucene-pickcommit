package org.apache.lucene.index;

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

/**
 * Since merger.merge is not public, we need to build
 * this tool under this namespace
 */
public class SegmentMergeTool {

    private final static Logger logger = LoggerFactory.getLogger(SegmentMergeTool.class.getName());

    public static class Config {
        public String segmentPath;
        public List<Integer> deleteSegs;
        public String searchTerm;
        public List<Integer> mergeSegs;
        public boolean showSegmentCommitInfo;
        public boolean showSegmentInfo;
    }

    // TODO : We should skip last argument. its a path
    private final Directory dir;
    private final DirectoryReader dreader;
    private final SegmentInfos sis;


    public SegmentMergeTool (Config cfg) throws IOException {
        // prepare required fileds
        dir = FSDirectory.open(Paths.get(cfg.segmentPath));
        dreader = DirectoryReader.open(dir);
        String files[]  = dir.listAll();
        String lastSegmentFile = SegmentInfos.getLastCommitSegmentsFileName(files);
        sis = SegmentInfos.readCommit(dir, lastSegmentFile);
    }


    public void showSegmentCommitInfo() throws IOException {
        // segment info

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
    }

    private void showSegmentInfo() throws IOException {

        IndexCommit ic = dreader.getIndexCommit();
        logger.info("segment count = {}", ic.getSegmentCount());
        logger.info("files= {}", ic.getFileNames());
        logger.info("generation = {}", ic.getGeneration());
        logger.info("segment file name = {}", ic.getSegmentsFileName());
        logger.info("isDeleted?? : {}", ic.isDeleted());
        for (Map.Entry<String, String> entry: ic.getUserData().entrySet()) {
            logger.info("{} {}", entry.getKey(), entry.getValue());
        }
    }

    public void exec(Config cfg) throws IOException {

        if (cfg.showSegmentInfo) {
            showSegmentInfo();
        }

        if (cfg.showSegmentCommitInfo) {
            showSegmentCommitInfo();
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
            List<CodecReader>  segmentsToMerge = Arrays.asList(); // SegmentReader <: CodecReader
            IOContext ctx = new IOContext(
                    new MergeInfo(-1, -1, false, -1));
            for (int segid: cfg.mergeSegs) {
                SegmentCommitInfo si  = sis.info(segid);
                segmentsToMerge.add(new SegmentReader(si, ctx));
            }

            final Codec codec = Codec.getDefault();
            final SegmentInfo newSegment = new SegmentInfo(
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
                    segmentsToMerge,
                    newSegment,
                    InfoStream.getDefault(),
                    trackingDir,
                    new FieldInfos.FieldNumbers(),
                    ctx

            );
            MergeState ms = merger.merge();
            logger.info("merge files:{}", trackingDir.getCreatedFiles());
            SegmentCommitInfo infoPerCommit = new SegmentCommitInfo(newSegment, 0, -1L, -1L, -1L);
            newSegment.setFiles(new HashSet<>(trackingDir.getCreatedFiles()));
            trackingDir.clearCreatedFiles();

            // created segment info
            codec.segmentInfoFormat().write(trackingDir, newSegment, ctx);
            logger.info("seg info files:{}", trackingDir.getCreatedFiles());
            newSegment.addFiles(trackingDir.getCreatedFiles());

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
