package com.example.springbatchexample.part3;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.List;

//ListItemReader와 동일하다.
public class CustomItemReader<T> implements ItemReader<T> {
    private final List<T> items;

    public CustomItemReader(List<T> items) {
        this.items = items;
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if(!items.isEmpty()) {
            return items.remove(0);
        }
        // null을 리턴하면 chunk 반복의 끝이다.
        return null;
    }
}
