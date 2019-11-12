package org.apache.lucene.index;

import com.github.shinpei.commitpick.LoggerPrintStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

/**
 * Since merger.merge is not public, we need to build
 * this tool under this namespace
 */
public class CommitPickTool {

    private final static Logger logger = LoggerFactory.getLogger(CommitPickTool.class.getName());

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

    private final PrintWriter pw;

    public CommitPickTool(String segmentPath) throws IOException {

        this(FSDirectory.open(Paths.get(segmentPath)), new LoggerPrintStream(logger));
    }

    public CommitPickTool(Directory dir) throws IOException {
        this(dir, new LoggerPrintStream(logger));
    }


    public CommitPickTool(Directory dir, PrintStream givenPs) throws IOException {
        // prepare required fields
        this.dir = dir;

        dreader = DirectoryReader.open(dir);

        PrintStream ps = givenPs == null ? System.out: givenPs;
        pw = new PrintWriter(ps, true);

        String files[]  = dir.listAll();
        String lastSegmentFile = SegmentInfos.getLastCommitSegmentsFileName(files);
        sis = SegmentInfos.readCommit(dir, lastSegmentFile);
    }


    public void showSegmentCommitInfo() throws IOException {
        // segment info
        int numSegments = sis.asList().size();
        StringBuilder sb = new StringBuilder();
        logger.info("num of segments = {}", numSegments);
        for (int i = 0; i < numSegments; i++) {
            final SegmentCommitInfo info = sis.info(i);
            sb.append(String.format("[%d, %s] ", i, info.info.name))
                    .append(String.format("maxDoc=%d, ", info.info.maxDoc()));
            Sort indexSort = info.info.getIndexSort();
            if (indexSort !=null) {
                sb.append(String.format("indexSort=%s, ", indexSort));
            }

            sb.append(String.format("size=%f[mb], ", info.sizeInBytes()/(1024. * 1024.)));

            /*
            Map<String, String> diag = info.info.getDiagnostics();
            for (Map.Entry<String, String> e: diag.entrySet()) {
                logger.info("{} -> {}", e.getKey(), e.getValue());
            }
            */

            if (!info.hasDeletions()) {
                sb.append(String.format("delete=0"));
            } else {
                sb.append(String.format("delete=%d", info.getDelCount()));
            }
            logger.info("{}", sb.toString());
            sb.setLength(0);
        }

    }

    public void testPS() {
        pw.format("PS IS WORKING!!!!!\n");
        pw.println();
    }

    public void showSegmentInfo() throws IOException {
        IndexCommit ic = dreader.getIndexCommit();
        pw.format("segment count = %d\n", ic.getSegmentCount());
        pw.format("files= %s\n", ic.getFileNames());
        pw.format("generation = %d\n", ic.getGeneration());
        pw.format("segment file name = %s\n", ic.getSegmentsFileName());
        pw.format("isDeleted?? : %s\n", ic.isDeleted());
        for (Map.Entry<String, String> entry: ic.getUserData().entrySet()) {
            pw.format("%s: %s\n", entry.getKey(), entry.getValue());
        }
    }

    public void deleteCommit(List<Integer> deleteSegs) throws IOException {

        pw.format("Invoking deletes, files= %s\n", Arrays.toString(deleteSegs.toArray()));
        for (int segidx : deleteSegs) {
            SegmentCommitInfo sci = sis.info(segidx);
            pw.format("Removing: %s\n", sci.info.files());
            sis.remove(segidx);
        }
        // Write out metadata.
        sis.prepareCommit(dir);
        sis.finishCommit(dir);
    }

    public void searchTerm(final String searchTerm) throws IOException {
        // Search
        IndexSearcher searcher = new IndexSearcher(dreader);

        BooleanQuery query = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("name", searchTerm)), BooleanClause.Occur.MUST)
                .build();
        TopDocs topDocs = searcher.search(query, 1);
        pw.format("Total hits: %d, scoreDocs:%d\n", topDocs.totalHits, topDocs.scoreDocs.length);
    }

    public void mergeCommit(List<Integer> mergeSegs) throws IOException {
        long start = System.nanoTime();
        logger.info("Merging...");
        List<CodecReader> segmentsToMerge = new ArrayList<>(); // SegmentReader <: CodecReader
        IOContext ctx = new IOContext(
                new MergeInfo(-1, -1, false, -1));
        for (int segid: mergeSegs) {
            SegmentCommitInfo si  = sis.info(segid);
            segmentsToMerge.add(new SegmentReader(si, ctx));
        }

        final Codec codec = Codec.getDefault();
        final SegmentInfo newSegment = new SegmentInfo(
                dir,
                Version.LATEST,
                "_pei8",
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
        // Write out metadata.
        sis.prepareCommit(dir);
        sis.finishCommit(dir);
        long end = System.nanoTime();
        logger.info("Elapsed merging: {}[s]", (end - start)/(1000.*1000.*1000.));
    }
}
