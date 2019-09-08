package org.ximo.finkinaction.java.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author xikl
 * @date 2019/9/8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student {

    private Integer id;

    private String name;

    private Integer age;

    public Student(String name, Integer age) {
        this.name = name;
        this.age = age;
    }
}
