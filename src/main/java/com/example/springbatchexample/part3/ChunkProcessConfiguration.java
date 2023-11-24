package com.example.springbatchexample.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ChunkProcessConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job chunkProcessingJob() {
        return jobBuilderFactory.get("chunkProcessingJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskBaseStep())
                .next(this.chunkBaseStep(null))
                .build();
    }

    @Bean
    public Step taskBaseStep() {
        return stepBuilderFactory.get("taskBaseStep").
                tasklet(this.tasklet(null))
                .build();
    }

    // tasklet을 chunk 방식 처럼 사용 하는 예제
    @Bean
    @StepScope // @StepScope를 제거하면 jobParameters를 사용할 수 없다.
    public Tasklet tasklet(@Value("#{jobParameters[chunkSize]}") String value) {
        List<String> items = getItems();

        return ((contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();
            JobParameters jobParameters = stepExecution.getJobParameters();

            int chunkSize = (value != null)? Integer.parseInt(value) : 10;
            log.info("Chunk Size : {}", Integer.toString(chunkSize));

            int fromIndex = stepExecution.getReadCount();
            int toIndex = fromIndex + chunkSize;

            if(fromIndex >= items.size()) {
                return RepeatStatus.FINISHED;
            }

            List<String> subList = items.subList(fromIndex, toIndex);
            log.info("task item size : {}", subList.size());

            stepExecution.setReadCount(toIndex);

            // 반복적으로 수행하기 위해서 RepeatStatus.CONTINUABLE 리턴
            return RepeatStatus.CONTINUABLE;
        });
    }

    private List<String> getItems() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            items.add(i + " Hello");
        }

        return items;
    }

    //reader에서 null을 return 할 때 까지 Step을 반복
    //<INPUT, OUTPUT> processsor에서는 input을 받아 output을 리턴
    //writer에서는 일괄 처리(chunk 단위 만큼)
    @Bean
    //Scope는 Bean의 라이프 사이클을 명시하므로, 반드시 Bean이어야 한다.
    //@JobScope는 job 실행 시점에 생성/소멸 - Step에 선언
    //@StepScope는 step 실행 시점에 생성/소멸 - Tasklet, Chunk 등에 선언
    //JobParameter를 사용하기 위해서는 @JobScope, @StepScope 반드시 선언되어 있어야 한다.
    @JobScope
    public Step chunkBaseStep(@Value("#{jobParameters[chunkSize]}") String chunkSize){
        return stepBuilderFactory.get("chunkBaseStep")
                // 100개의 데이터를 10개씩 나누어서 실행해라
                .<String, String>chunk((chunkSize != null) ? Integer.parseInt(chunkSize) : 10)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    private ItemWriter<? super String> itemWriter() {

        return items -> {
            // processor를 통해서 변경된 문자가 넘어온 것을 확인할 수 있다.
            items.forEach(log::info);
            log.info("chunk item size : {}", items.size());
        };
    }

    // null을 리턴하면 writer로 넘어가지 않는다.
    private ItemProcessor<? super String, String> itemProcessor() {
        return item -> item + ", Spring Batch";
    }


    // 배치 대상 데이터를 읽기 위한 설정  - 파일, DB, 네트워크 등에서 읽기 위함
    // 기본 제공되는 ItemReader 구현체 -
    private ItemReader<String> itemReader() {
        // List 컬렉션을 읽을 수 있는 Reader
        return new ListItemReader<>(getItems());
    }



}
