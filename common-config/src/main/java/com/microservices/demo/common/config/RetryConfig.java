package com.microservices.demo.common.config;

import com.microservices.demo.config.RetryConfigData;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RetryConfig {

    private final RetryConfigData retryConfigData;
}
