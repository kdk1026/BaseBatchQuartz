package com.kdk.app.db.batch;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisPagingItemReader;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.PlatformTransactionManager;

import com.kdk.app.db.batch.reader.CityVoPagingReader;
import com.kdk.app.db.batch.writer.CityVoItemWriter;
import com.kdk.app.db.vo.CityVo;

import lombok.extern.slf4j.Slf4j;

/**
 * <pre>
 * -----------------------------------
 * 개정이력
 * -----------------------------------
 * 2025. 1. 29. kdk	최초작성
 * </pre>
 *
 *
 * @author kdk
 */
@Slf4j
//@Configuration
//@EnableBatchProcessing
public class DbBatchConfiguration {

    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    private static final int PAGE_SIZE_AND_CHUNK_SIZE = 1000;
    private static final String JOB_NAME = "importCityJob";

    @Bean
    MyBatisPagingItemReader<CityVo> dbReader() {
    	CityVoPagingReader reader = new CityVoPagingReader();

        reader.setSqlSessionFactory(sqlSessionFactory);
        reader.setQueryId("com.kdk.app.db.mapper.CityMapper.selectCityAll");
        reader.setPageSize(PAGE_SIZE_AND_CHUNK_SIZE);
        return reader;
    }

    @Bean
    ItemProcessor<CityVo, CityVo> dbProcessor() {
        return new ItemProcessor<CityVo, CityVo>() {
            @Override
            public CityVo process(CityVo item) throws Exception {
                return item;
            }
        };
    }

    @Bean
    CityVoItemWriter dbWriter(SqlSessionFactory sqlSessionFactory) {
        return new CityVoItemWriter(sqlSessionFactory);
    }

    @Primary
    @Bean
    Step dbStep1(JobRepository dbJobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("dbStep1", dbJobRepository)
                .<CityVo, CityVo>chunk(PAGE_SIZE_AND_CHUNK_SIZE, transactionManager)
                .reader(dbReader())
                .processor(dbProcessor())
                .writer(dbWriter(sqlSessionFactory))
                .listener(new StepExecutionListener() {

					@Override
					public void beforeStep(StepExecution stepExecution) {
						try ( SqlSession sqlSession = sqlSessionFactory.openSession() ) {
							sqlSession.delete("com.kdk.app.db.mapper.CityMapper.deleteCityBackAll");
							sqlSession.commit();
						} catch (Exception e) {
							log.error("", e);
							throw e;
						}
					}

					@Override
					public ExitStatus afterStep(StepExecution stepExecution) {
						return stepExecution.getExitStatus();
					}

				})
                .build();
    }

    @Bean
    Job importCityJob(JobRepository dbJobRepository, JobCompletionNotificationListener dbListener, Step dbStep1) {
        return new JobBuilder(JOB_NAME, dbJobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(dbListener)
                .start(dbStep1)
                .build();
    }

    @Bean
    JobParameters defaultJobParameters() {
        return new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();
    }

    @Bean
    JobCompletionNotificationListener dbListener() {
        return new JobCompletionNotificationListener();
    }

    public class JobCompletionNotificationListener implements JobExecutionListener {

		@Override
		public void beforeJob(JobExecution jobExecution) {
			log.info("Job is starting with parameters: {}", jobExecution.getJobParameters());
		}

		@Override
		public void afterJob(JobExecution jobExecution) {
			if ( jobExecution.getStatus() == BatchStatus.COMPLETED ) {
				log.info("DbBatch Job completed successfully!");
			} else if ( jobExecution.getStatus() == BatchStatus.FAILED || jobExecution.getStatus() == BatchStatus.STOPPED ) {
				log.error("Job failed or stopped with status: {}", jobExecution.getAllFailureExceptions());
			}
		}

    }


}
