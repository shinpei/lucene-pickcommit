package com.github.shinpei.commitpick;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class LoggerPrintStream extends PrintStream {

    final Logger logger;
    private final Logger innerLogger = LoggerFactory.getLogger(LoggerPrintStream.class.getName());
    private ByteArrayOutputStream bout;
    private int last = -1;

    public LoggerPrintStream(Logger logger) {
        super(new ByteArrayOutputStream(), true);
        bout = (ByteArrayOutputStream)super.out;
        this.logger = logger;
        innerLogger.info("HIHIHI");
    }

    @Override
    public void write(byte buf[], int off, int len) {
        innerLogger.info("HIHIHI2");
        for (int i = 0; i < len; i++) {
            write(buf[off + i]);
        }
        logger.info("'{}'", new String(buf, off, len).trim());
    }

    public void write(int b) {
        if ((last == '\r') && (b == '\n')) {
            last = -1;
            return;
        } else if ((b == '\n') || (b == '\r')) {
            try {
                /* write the converted bytes of the log message */
                String message =
                        Thread.currentThread().getName() + ": " +
                                bout.toString();
                logger.info("{}", message);
            } finally {
                bout.reset();
            }
        } else {
            super.write(b);
        }
        last = b;
    }
}
