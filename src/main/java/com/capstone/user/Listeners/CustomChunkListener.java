package com.capstone.user.Listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

@Slf4j
public class CustomChunkListener implements ChunkListener {

    //private static long counter = 0L;

    @Override
    public void afterChunk(ChunkContext chunkContext) {

        log.info(
                "READ: " + chunkContext.getStepContext().getStepExecution().getReadCount() +
                " records for STEP " + "\"" + chunkContext.getStepContext().getStepName() + "\"" +
                " in JOB " + "\"" + chunkContext.getStepContext().getStepExecution().getJobExecution().getJobInstance().getJobName() + "\""
        );

        log.info(
                "FILTERED: " + chunkContext.getStepContext().getStepExecution().getFilterCount() +
                " records for STEP " + "\"" + chunkContext.getStepContext().getStepName() + "\"" +
                " in JOB " + "\"" + chunkContext.getStepContext().getStepExecution().getJobExecution().getJobInstance().getJobName() + "\""
        );

        log.info(
                "WROTE: " + chunkContext.getStepContext().getStepExecution().getWriteCount() +
                " records for STEP " + "\"" + chunkContext.getStepContext().getStepName() + "\"" +
                " in JOB " + "\"" + chunkContext.getStepContext().getStepExecution().getJobExecution().getJobInstance().getJobName() + "\""
        );
        log.info("------------------------------------------------------------------");

        //counter++;

        //chunkContext.getStepContext().getJobParameters().get("userId");

    }

    @Override
    public void afterChunkError(ChunkContext chunkContext) {
        log.error("Chunk error at step: " + chunkContext.getStepContext().getStepName());
    }

}
