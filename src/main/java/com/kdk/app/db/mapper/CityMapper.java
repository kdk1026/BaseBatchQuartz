package com.kdk.app.db.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.kdk.app.db.vo.CityVo;

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
@Mapper
public interface CityMapper {

	public List<CityVo> selectCityAll();

	public int deleteCityBackAll();

	public int insertCityBack(CityVo vo);

}
