package com.genius.springbatchdemo;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;

import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.batch.core.Job;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
@Configuration
@EnableBatchProcessing
public class ReadTextFileConfiguration {

	@Value("${async.thread.max.pool}")
	private Integer maxPoolSize;

	@Value("${async.thread.core.pool}")
	private Integer corePoolSize;

	@Value("${async.thread.queue}")
	private Integer queueSize;
	
	@Value("${input.file.path}")
	private String inputFilePath;
	
	@Value("${output.file.path}")
	private String outputFilePath;
	
	@Autowired
    private StepBuilderFactory steps;

	@Bean
	ItemReader<String> reader() {
		FlatFileItemReader<String> csvFileReader = new FlatFileItemReader<>();
		csvFileReader.setResource(new ClassPathResource(inputFilePath));
		csvFileReader.setLinesToSkip(1);

		csvFileReader.setLineMapper(new PassThroughLineMapper());

		return csvFileReader;
	}

	@Bean
	ItemProcessor<String, String> processor() {
		ItemProcessor<String, String> itemProcessor = new ItemProcessor<String, String>() {

			@Override
			public String process(String item) throws Exception {
				// TODO DO ENCRYPTION HERE
				//CeaserCipher.cipher(item);
				return CeaserCipher.cipher(item);
			}
		};
		return itemProcessor;
	}

	@Bean
	ItemWriter<String> writer() {
		final Logger LOGGER = LoggerFactory.getLogger(ReadTextFileConfiguration.class);
		ItemWriter<String> itemWriter = new ItemWriter<String>() {

			@Override
			public void write(List<? extends String> items) throws Exception {
				LOGGER.info("Received the information of {} students", items.size());

				items.forEach(i -> LOGGER.debug("Received the information of a student: {}", i));

			}
		};
		return itemWriter;
	}
	
	@Bean
	public FlatFileItemWriter fileItemWriter() {
	        return  new FlatFileItemWriterBuilder<String>()
	                                   .name("itemWriter")
	                                   .resource(new FileSystemResource(outputFilePath))
	                                   .lineAggregator(new PassThroughLineAggregator<>())
	                                   .build();
	}
	@Bean(name = "asyncExecutor")
    public TaskExecutor getAsyncExecutor()
    {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(corePoolSize);
        executor.setQueueCapacity(queueSize);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("AsyncExecutor-");
        return executor;
    }

	@Bean
	public ItemProcessor<String,Future<String>> asyncItemProcessor() {
		AsyncItemProcessor<String, String> asyncItemProcessor = new AsyncItemProcessor<>();
		asyncItemProcessor.setDelegate(processor());
		asyncItemProcessor.setTaskExecutor(getAsyncExecutor());
		return asyncItemProcessor;
	}

	@Bean
	public ItemWriter<Future<String>> asyncItemWriter() {
		AsyncItemWriter<String> asyncItemWriter = new AsyncItemWriter<>();
		asyncItemWriter.setDelegate(fileItemWriter());
		return asyncItemWriter;
	}
	
	@Bean
    protected Step step1(){
        return this.steps.get("step1")
                .<String, Future<String>> chunk(corePoolSize)
                .reader(reader())
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                .build();
    }


	@Bean
	Job myJob(JobBuilderFactory jobBuilderFactory, @Qualifier("step1") Step inMemoryStudentStep) {

		return jobBuilderFactory.get("myJob").incrementer(new RunIdIncrementer()).flow(inMemoryStudentStep).end()
				.build();

	}

}
