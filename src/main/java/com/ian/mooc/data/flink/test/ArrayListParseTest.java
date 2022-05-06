package com.ian.mooc.data.flink.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ArrayListParseTest {
    public static void main(String[] args) {
        String str = new String("[[\"\",\"12\"],[\"21\",\"22\"]]");
        List<String> list = Collections.singletonList(str);

        System.out.println(list);
    }

}
