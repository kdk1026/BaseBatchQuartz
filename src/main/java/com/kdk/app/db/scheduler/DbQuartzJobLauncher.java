package com.kdk.app.db.scheduler;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;

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
public class DbQuartzJobLauncher implements Job {

	@Autowired
	private JobLauncher jobLauncher;

    @Autowired
    private org.springframework.batch.core.Job importCityJob;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(importCityJob, jobParameters);
		} catch (Exception e) {
			log.error("", e);
		}
	}

}