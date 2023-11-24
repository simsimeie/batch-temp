package com.example.springbatchexample.part1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class HelloConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public HelloConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job helloJob(){
        return jobBuilderFactory.get("helloJob")
                // 새로운 job instance를 생성하도록 해준다.
                .incrementer(new RunIdIncrementer())
                .start(this.helloStep())
                .build();
    }


    // 하나의 job에는 여러 개의 step이 들어갈 수 있다.
    // step
    // chunk : 큰 덩어리를 n개씩 나눠서 실행 (10000개의 데이터를 1000개씩 나눠서 처리할 수 있다.)
        // chunk 기반 step은 ItemReader, ItemProcessor, ItemWriter가 있다.
    // tasklet : 하나의 작업 기반으로 실행
    @Bean
    public Step helloStep(){
        return stepBuilderFactory.get("helloStep")
                .tasklet((contribution, chunkContext) -> {
                    log.info("hello spring batch");
                    return RepeatStatus.FINISHED;
                }).build();
    }


}