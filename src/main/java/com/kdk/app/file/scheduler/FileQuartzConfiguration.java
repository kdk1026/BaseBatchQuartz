package com.kdk.app.file.scheduler;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
@Configuration
public class FileQuartzConfiguration {

    @Bean
    JobDetail fileJobDetail() {
        return JobBuilder.newJob(FileQuartzJobLauncher.class)
                .withIdentity("fileJobDetail")
                .storeDurably()
                .build();
    }

    @Bean
    Trigger fileTrigger() {
        return TriggerBuilder.newTrigger()
                .forJob(fileJobDetail())
                .withIdentity("fileTrigger")
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0/2 * * * ?"))
                .build();
    }

}
