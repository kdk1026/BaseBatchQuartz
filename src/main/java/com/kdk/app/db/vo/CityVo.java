package com.kdk.app.db.vo;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

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
@Getter
@Setter
@ToString
public class CityVo {

	private String id;
	private String name;
	private String countryCode;
	private String district;
	private String population;

}
