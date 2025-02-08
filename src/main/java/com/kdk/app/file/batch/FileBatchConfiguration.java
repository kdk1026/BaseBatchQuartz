package com.kdk.app.file.batch;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.kdk.app.file.batch.reader.VirtualAccountVoFileReader;
import com.kdk.app.file.batch.writer.VirtualAccountVoFileWriter;
import com.kdk.app.file.vo.VirtualAccountVo;

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
@Configuration
@EnableBatchProcessing
public class FileBatchConfiguration {

    @Autowired
    private SqlSessionFactory sqlSessionFactory;

    private static final String JOB_NAME = "importAccountJob";
    private static final int CHUNK_SIZE = 10;

    @Bean
    FlatFileItemReader<VirtualAccountVo> fileReader() {
        return new VirtualAccountVoFileReader("C:/test/acount.txt");
    }

    @Bean
    ItemProcessor<VirtualAccountVo, VirtualAccountVo> fileProcessor() {
        return new ItemProcessor<VirtualAccountVo, VirtualAccountVo>() {

			@Override
			public VirtualAccountVo process(VirtualAccountVo item) throws Exception {
				// 필요한 데이터 변환 로직을 작성할 수 있습니다.
				return item;
			}

        };
    }

    @Bean
    VirtualAccountVoFileWriter fileWriter(SqlSessionFactory sqlSessionFactory) {
        return new VirtualAccountVoFileWriter(sqlSessionFactory);
    }

    @Bean
    Step fileStep1(JobRepository fileJobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("fileStep1", fileJobRepository)
                .<VirtualAccountVo, VirtualAccountVo>chunk(CHUNK_SIZE, transactionManager)
                .reader(fileReader())
                .processor(fileProcessor())
                .writer(fileWriter(sqlSessionFactory))
                .listener(new StepExecutionListener() {

					@Override
					public void beforeStep(StepExecution stepExecution) {
						try ( SqlSession sqlSession = sqlSessionFactory.openSession() ) {
							sqlSession.delete("com.kdk.app.file.mapper.VirtualAccountMapper.deleteVirtualAccount");
							sqlSession.commit();
						} catch (Exception e) {
							log.error("", e);
							throw e;
						}
					}

					@Override
					public ExitStatus afterStep(StepExecution stepExecution) {
						log.info("Step 완료: {}, 상태: {}", stepExecution.getStepName(), stepExecution.getStatus());
						log.info("Step Execution Context: {}", stepExecution.getExecutionContext());
						return stepExecution.getExitStatus();
					}

				})
                .build();
    }

    @Bean
    Job importAccountJob(JobRepository fileJobRepository, JobCompletionNotificationListener fileListener, Step fileStep1) {
        return new JobBuilder(JOB_NAME, fileJobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(fileListener)
                .start(fileStep1)
                .build();
    }

    @Bean
    JobCompletionNotificationListener fileListener() {
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
				log.info("fileBatch Job completed successfully!");
			} else if ( jobExecution.getStatus() == BatchStatus.FAILED || jobExecution.getStatus() == BatchStatus.STOPPED ) {
				log.error("Job failed or stopped with status: {}", jobExecution.getAllFailureExceptions());
			}
		}

    }

}
