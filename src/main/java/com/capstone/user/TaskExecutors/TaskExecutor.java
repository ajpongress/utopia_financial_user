package com.capstone.user.TaskExecutors;

import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class TaskExecutor {

    // Match available system resources
    int corePoolSize = Runtime.getRuntime().availableProcessors();

    // Multithreading taskExecutor (using SimpleAsyncTaskExecutor)
    @Bean
    public org.springframework.core.task.TaskExecutor asyncTaskExecutor() {

        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(corePoolSize);
        return simpleAsyncTaskExecutor;
    }
}
