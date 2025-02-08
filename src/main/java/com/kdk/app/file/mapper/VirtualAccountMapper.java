package com.kdk.app.file.mapper;

import org.apache.ibatis.annotations.Mapper;

import com.kdk.app.file.vo.VirtualAccountVo;

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
public interface VirtualAccountMapper {

	public int deleteVirtualAccount();

	public int insertVirtualAccount(VirtualAccountVo vo);


}
