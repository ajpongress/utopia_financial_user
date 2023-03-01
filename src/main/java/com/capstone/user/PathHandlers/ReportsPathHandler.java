package com.capstone.user.PathHandlers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@StepScope
@Component
@Slf4j
public class ReportsPathHandler {

    @Value("#{jobParameters['reportsPath_param']}")
    String reportsPath;

    public String getReportsPath() {
        return reportsPath;
    }
}
