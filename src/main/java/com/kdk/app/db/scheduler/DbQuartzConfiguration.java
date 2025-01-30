package com.kdk.app.db.scheduler;

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
//@Configuration
public class DbQuartzConfiguration {

    @Bean
    JobDetail dbJobDetail() {
        return JobBuilder.newJob(DbQuartzJobLauncher.class)
                .withIdentity("dbJobDetail")
                .storeDurably()
                .build();
    }

    @Bean
    Trigger dbTrigger() {
        return TriggerBuilder.newTrigger()
                .forJob(dbJobDetail())
                .withIdentity("dbTrigger")
                .withSchedule(CronScheduleBuilder.cronSchedule("0 0/2 * * * ?"))
                .build();
    }

}
