package com.kdk.app.file.batch.writer;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import com.kdk.app.file.vo.VirtualAccountVo;

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
public class VirtualAccountVoFileWriter implements ItemWriter<VirtualAccountVo> {

	private SqlSessionFactory sqlSessionFactory;

    public VirtualAccountVoFileWriter(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

	@Override
	public void write(Chunk<? extends VirtualAccountVo> chunk) throws Exception {
        log.info("Writing " + chunk.size() + " items.");
        try ( SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false) ) {
            for ( VirtualAccountVo item : chunk ) {
                sqlSession.insert("com.kdk.app.file.mapper.VirtualAccountMapper.insertVirtualAccount", item);
            }
            sqlSession.commit();
        } catch (Exception e) {
            log.error("Error during batch operation", e);
            throw e;
        }
	}

}
