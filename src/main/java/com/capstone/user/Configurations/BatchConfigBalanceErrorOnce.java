package com.capstone.user.Configurations;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.BalanceErrorOnceProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;
import java.util.List;

@Configuration
@Slf4j
public class BatchConfigBalanceErrorOnce {

    // ----------------------------------------------------------------------------------
    // --                                  SETUP                                       --
    // ----------------------------------------------------------------------------------

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    @Qualifier("reader_Transaction_User")
    private SynchronizedItemStreamReader<UserTransactionModel> synchronizedItemStreamReader;

    @Autowired
    private BalanceErrorOnceProcessor balanceErrorOnceProcessor;

    @Autowired
    @Qualifier("writer_Transaction_User")
    private ClassifierCompositeItemWriter<UserTransactionModel> classifierCompositeItemWriter;

    @Autowired
    @Qualifier("taskExecutor_User")
    private org.springframework.core.task.TaskExecutor asyncTaskExecutor;

    @Autowired
    private UserTransactionClassifier userTransactionClassifier;


    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - insufficient balance (at least once)
    @Bean
    public Step step_exportFirstBalanceError() {

        return new StepBuilder("balanceErrorOnceStep", jobRepository)
                .<UserTransactionModel, UserTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(balanceErrorOnceProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {



                        userTransactionClassifier.closeAllwriters();
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

//                        stepExecution.getExecutionContext().put("counters", new List<>() {
//                        });

//                        try {
//                            File insufficientBalanceDirectory = new File("src/main/resources/insufficient_balance_once");
//                            long fileCount = insufficientBalanceDirectory.list().length;
//
//                        } catch (NullPointerException e) {
//                            return new ExitStatus("" + e);
//                        }

//                        log.info("------------------------------------------------------------------");
//
//                        log.info("------------------------------------------------------------------");
                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - insufficient balance (at least once)
    @Bean
    public Job job_exportFirstBalanceError() {

        return new JobBuilder("balanceErrorOnceJob", jobRepository)
                .start(step_exportFirstBalanceError())
                .build();
    }
}
