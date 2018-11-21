package com.github.shinpei.commitpick;

import com.google.common.base.Splitter;
import org.apache.lucene.index.CommitPickTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parse arguments, propagate config to Segmerge tool
 */
public class Main {
    private final static Logger logger = LoggerFactory.getLogger(CommitPickTool.class.getName());

    static final Splitter COMMA_SPLITTER = Splitter.on(",");

    public static void main (String[] args) throws IOException {
        List<String> argL = Arrays.asList(args);

        String segmentPath = "./";

        final CommitPickTool.Config cfg = new CommitPickTool.Config();

        boolean parseOnce; // otherwise, its 2-2
        for (int i = 0; i < argL.size(); i+= parseOnce ? 1: 2) {
            parseOnce = false;
            switch (argL.get(i)) {
                case "--delete":
                    for (String s : COMMA_SPLITTER.split((argL.get(i+1)))) {
                        if (cfg.deleteSegs == null) {
                            cfg.deleteSegs = new ArrayList<>();
                        }
                        cfg.deleteSegs.add(Integer.parseInt(s));
                    }
                    logger.info("Delete enabled for : {}", Arrays.toString(cfg.deleteSegs.toArray()));
                case "--query":
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
                case "--commit-info":
                    cfg.showSegmentCommitInfo = true;
                    parseOnce = true;
                    break;

                case "--segment-info":
                    cfg.showSegmentInfo = true;
                    parseOnce = true;
                    break;

                default:
                    // plus one is always a path
                    segmentPath =  argL.get(i);
                    parseOnce = true;
            }
        }
        cfg.segmentPath = segmentPath;
        logger.info("Set path to = {}", cfg.segmentPath);

        CommitPickTool tool = new CommitPickTool(cfg.segmentPath);

        if (cfg.showSegmentInfo) {
            tool.showSegmentInfo();
        }

        if (cfg.showSegmentCommitInfo) {
            tool.showSegmentCommitInfo();
        }

        if (cfg.deleteSegs != null) {
            tool.deleteCommit(cfg.deleteSegs);
        }

        if (cfg.mergeSegs != null) {
            tool.mergeCommit(cfg.mergeSegs);
        }

        if (cfg.searchTerm != null){
            tool.searchTerm(cfg.searchTerm);
        }


    }
}
