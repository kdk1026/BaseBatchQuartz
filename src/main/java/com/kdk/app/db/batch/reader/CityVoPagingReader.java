package com.kdk.app.db.batch.reader;

import java.util.HashMap;
import java.util.Map;

import org.mybatis.spring.batch.MyBatisPagingItemReader;

import com.kdk.app.db.vo.CityVo;

import lombok.extern.slf4j.Slf4j;

/**
 * <pre>
 * -----------------------------------
 * 개정이력
 * -----------------------------------
 * 2025. 2. 8. kdk	최초작성
 * </pre>
 *
 *
 * @author kdk
 */
@Slf4j
public class CityVoPagingReader extends MyBatisPagingItemReader<CityVo> {

	private Map<String, Object> parameterValues = new HashMap<>();

	@Override
	protected void doReadPage() {
        log.info("Reading page: " + getPage());
        int offset = getPage() * getPageSize();
        int limit = getPageSize();
        parameterValues.put("offset", offset);
        parameterValues.put("limit", limit);
        log.info("Setting parameter values: offset = " + offset + ", limit = " + limit);
        setParameterValues(parameterValues);
        super.doReadPage();
	}



}
