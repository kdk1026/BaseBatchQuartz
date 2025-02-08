package com.kdk.app.file.batch.reader;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DefaultFieldSet;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.core.io.FileSystemResource;

import com.kdk.app.file.vo.VirtualAccountVo;

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
public class VirtualAccountVoFileReader extends FlatFileItemReader<VirtualAccountVo> {

    public VirtualAccountVoFileReader(String filePath) {
        setResource(new FileSystemResource(filePath));
        setLinesToSkip(0); // 첫 줄 건너뛰지 않음

        DefaultLineMapper<VirtualAccountVo> lineMapper = new DefaultLineMapper<>();

//      csv 파일 등
//      DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//      tokenizer.setDelimiter(",");
//      tokenizer.setNames("필드1", "필드2");

        LineTokenizer tokenizer = new LineTokenizer() {
            @Override
            public FieldSet tokenize(String line) {
                return new DefaultFieldSet(new String[] {line}, new String[] {"account"});
            }
        };

        BeanWrapperFieldSetMapper<VirtualAccountVo> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(VirtualAccountVo.class);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        setLineMapper(lineMapper);
    }

}
