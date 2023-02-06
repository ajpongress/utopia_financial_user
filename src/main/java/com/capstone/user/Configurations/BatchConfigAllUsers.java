package com.capstone.user.Configurations;

import com.capstone.user.Classifiers.UserTransactionClassifier;
import com.capstone.user.Models.UserTransactionModel;
import com.capstone.user.Processors.AllUsersProcessor;
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
public class BatchConfigAllUsers {

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
    private AllUsersProcessor allUsersProcessor;

    @Autowired
    @Qualifier("writer_Transaction_User")
    private ClassifierCompositeItemWriter<UserTransactionModel> classifierCompositeItemWriter;

    @Autowired
    private TaskExecutor taskExecutor;

    @Autowired
    private UserTransactionClassifier userTransactionClassifier;

    // ----------------------------------------------------------------------------------
    // --                             STEPS & JOBS                                     --
    // ----------------------------------------------------------------------------------

    // Step - all user transactions
    @Bean
    public Step step_allUsers() {

        return new StepBuilder("allUsersStep", jobRepository)
                .<UserTransactionModel, UserTransactionModel> chunk(50000, transactionManager)
                .reader(synchronizedItemStreamReader)
                .processor(allUsersProcessor)
                .writer(classifierCompositeItemWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        userTransactionClassifier.closeAllwriters();
                        log.info("------------------------------------------------------------------");
                        log.info(stepExecution.getSummary());
                        log.info("------------------------------------------------------------------");
                        return StepExecutionListener.super.afterStep(stepExecution);
                    }
                })
                .taskExecutor(taskExecutor)
                .build();
    }

    // Job - all user transactions
    @Bean
    public Job job_allUsers() {

        return new JobBuilder("allUsersJob", jobRepository)
                .start(step_allUsers())
                .build();
    }
}
