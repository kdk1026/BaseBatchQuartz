package com.kdk.config.app;

import javax.sql.DataSource;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.mybatis.spring.boot.autoconfigure.SpringBootVFS;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * <pre>
 * -----------------------------------
 * 개정이력
 * -----------------------------------
 * 2025. 1. 30. kdk	최초작성
 * </pre>
 *
 *
 * @author kdk
 */
@Configuration
@MapperScan(basePackages = {"com.kdk.app.**.mapper"}, annotationClass = Mapper.class)
public class MyBatisConfig {

	private static final String MYBATIS_CONFIG_LOCATION = "mybatis/configuration.xml";
	private static final String MYBATIS_MAPPER_LOCATION = "classpath:mybatis/mappers/**/*.xml";

	private final DataSource dataSource;

    public MyBatisConfig(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@Bean
    SqlSessionFactory sqlSessionFactory() throws Exception {
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
