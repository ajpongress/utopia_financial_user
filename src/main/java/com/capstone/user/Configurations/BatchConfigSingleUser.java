package com.capstone.user.Configurations;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.SingleUserProcessor;
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
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Slf4j
public class BatchConfigSingleUser {

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
    private SingleUserProcessor singleUserProcessor;

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

    // Step - single user transactions
    @Bean
    public Step step_singleUser() {

        return new StepBuilder("singleUserStep", jobRepository)
                .<UserTransactionModel, UserTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(singleUserProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        userTransactionClassifier.closeAllwriters();
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");

                        singleUserProcessor.clearAllTrackersAndCounters();

                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(asyncTaskExecutor)
                .build();
    }

    // Job - single user transactions
    @Bean
    public Job job_singleUser() {

        return new JobBuilder("singleUserJob", jobRepository)
                .start(step_singleUser())
                .build();
    }
}
