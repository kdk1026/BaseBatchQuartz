package com.kdk.app.common.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 애플리케이션 설정 파일(예: application.properties, application.yml) 값을 조회
 *
 * <p>
 * 기존의 static 유틸리티 클래스 방식에서 벗어나, Spring의 의존성 주입(DI) 메커니즘을 활용하여
 * 더 견고하고 테스트하기 쉬운 구조를 제공
 * <p>
 *
 * <pre>
 * 개정이력
 * -----------------------------------
 * 2021. 8. 6. 	김대광	최초작성
 * 2025. 5. 24.	김대광	SonarQube 경고에 따른 Util 제거하고, Bean으로 생성
 * </pre>
 *
 * @author 김대광
 */
@Component
public class SpringBootProperty {

	private static final Logger logger = LoggerFactory.getLogger(SpringBootProperty.class);

	private final Environment environment;

	public SpringBootProperty(Environment environment) {
		this.environment = environment;
	}

	public String getProperty(String propertyName) {
		return getProperty(propertyName, null);
	}

	public String getProperty(String propertyName, String defaultValue) {
		if ( !StringUtils.hasText(propertyName) ) {
			throw new IllegalStateException("propertyName is null");
		}

		String value = "";

		String property = environment.getProperty(propertyName);
		if ( property == null ) {
			logger.warn("{} properties was not loaded.", propertyName);
			value = defaultValue;
		} else {
			value = property.trim();
		}

		return value;
	}

}
