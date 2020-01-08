package org.ximo.finkinaction.java.datastream.connectors;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @author xikl
 * @date 2020/1/8
 */
public class ObjectCommonSchema<T> extends AbstractDeserializationSchema<T>
        implements SerializationSchema<T> {

    /**
     * 必须要指定一个类型，否则似乎会报错{@link AbstractDeserializationSchema#AbstractDeserializationSchema()}中的异常信息
     *
     * @param type
     * @see AbstractDeserializationSchema 中的注释
     */
    public ObjectCommonSchema(Class<T> type) {
        super(type);
    }

    @Override
    public T deserialize(byte[] message) {
        return JSONObject.parseObject(message, new TypeReference<T>() {}.getType());
    }

    @Override
    public byte[] serialize(T element) {
        return JSONObject.toJSONBytes(element, SerializerFeature.WriteMapNullValue);
    }


}
