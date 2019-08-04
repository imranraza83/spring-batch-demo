package com.genius.springbatchdemo;

import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
public class BatchScheduler {
	@Bean
    public ResourcelessTransactionManager transactionManager1() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public MapJobRepositoryFactoryBean mapJobRepositoryFactoryBean(ResourcelessTransactionManager resourcelessTransactionManager) throws Exception {
        MapJobRepositoryFactoryBean factoryBean = new MapJobRepositoryFactoryBean(resourcelessTransactionManager);
        factoryBean.afterPropertiesSet();
        return factoryBean;
    }

    @Bean
    public JobRepository jobRepository1(MapJobRepositoryFactoryBean factoryBean) throws Exception{
        return (JobRepository) factoryBean.getObject();
    }

    @Bean
    public SimpleJobLauncher jobLauncher1(JobRepository jobRepository) {
        SimpleJobLauncher launcher = new SimpleJobLauncher();
        launcher.setJobRepository(jobRepository);
        return launcher;
    }
}
