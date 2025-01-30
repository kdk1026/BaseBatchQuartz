package com.kdk.config.app;

import javax.sql.DataSource;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.boot.autoconfigure.SpringBootVFS;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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
@EnableTransactionManagement
@MapperScan(basePackages = {"com.kdk.app.**.mapper"}, annotationClass = Mapper.class)
public class DataSourceConfig {

	private static final String MYBATIS_CONFIG_LOCATION = "mybatis/configuration.xml";
	private static final String MYBATIS_MAPPER_LOCATION = "classpath:mybatis/mappers/**/*.xml";

	@Bean
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    HikariConfig hikariConfig() {
		return new HikariConfig();
	}

    @Bean
    DataSource dataSource() {
    	return new HikariDataSource(this.hikariConfig());
	}

    @Bean
    PlatformTransactionManager transactionManager(DataSource dataSource) {
		return new DataSourceTransactionManager(dataSource);
	}

    @Bean
    SqlSessionFactory sqlSessionFactory(DataSource dataSource) throws Exception {
    	SqlSessionFactoryBean sqlSessionFactory = new SqlSessionFactoryBean();
		sqlSessionFactory.setDataSource(dataSource);
		sqlSessionFactory.setConfigLocation(new ClassPathResource(MYBATIS_CONFIG_LOCATION));
		sqlSessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(MYBATIS_MAPPER_LOCATION));
		sqlSessionFactory.setVfs(SpringBootVFS.class);
		return sqlSessionFactory.getObject();
	}

    @Bean
    SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory sqlSessionFactory) {
		return new SqlSessionTemplate(sqlSessionFactory);
	}

}
