package com.kdk.app.file.batch;

import org.apache.ibatis.session.ExecutorType;
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
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DefaultFieldSet;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

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
        FlatFileItemReader<VirtualAccountVo> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("C:/test/acount.txt"));
        reader.setLinesToSkip(0);	// 첫 줄 건너뛰지 않음

        DefaultLineMapper<VirtualAccountVo> lineMapper = new DefaultLineMapper<>();
//        csv 파일 등
//        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//        tokenizer.setDelimiter(",");
//        tokenizer.setNames("필드1", "필드2");

        LineTokenizer tokenizer = new LineTokenizer() {

			@Override
			public FieldSet tokenize(String line) {
				return new DefaultFieldSet(new String[] {line}, new String[] {"acount"});
			}

        };

        BeanWrapperFieldSetMapper<VirtualAccountVo> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(VirtualAccountVo.class);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLineMapper(lineMapper);
        return reader;
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
    ItemWriter<VirtualAccountVo> fileWriter() {
    	return new ItemWriter<VirtualAccountVo>() {

			@Override
			public void write(Chunk<? extends VirtualAccountVo> chunk) throws Exception {
				log.info("Writing " + chunk.size() + " items.");
				try ( SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false) ) {
					for ( VirtualAccountVo item : chunk ) {
						sqlSession.insert("com.kdk.app.file.mapper.VirtualAccountMaper.insertVirtualAccount", item);
					}

					sqlSession.commit();
				} catch (Exception e) {
					log.error("Error during batch operation", e);
                    throw e;
				}
			}

    	};
    }

    @Bean
    Step fileStep1(JobRepository fileJobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("fileStep1", fileJobRepository)
                .<VirtualAccountVo, VirtualAccountVo>chunk(CHUNK_SIZE, transactionManager)
                .reader(fileReader())
                .processor(fileProcessor())
                .writer(fileWriter())
                .listener(new StepExecutionListener() {

					@Override
					public void beforeStep(StepExecution stepExecution) {
						try ( SqlSession sqlSession = sqlSessionFactory.openSession() ) {
							sqlSession.delete("com.kdk.app.file.mapper.VirtualAccountMaper.deleteVirtualAccount");
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
