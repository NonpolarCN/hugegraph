/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.util;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public final class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        SimpleModule module = new SimpleModule();
        module.addSerializer(Date.class, new DateSerializer());
        module.addDeserializer(Date.class, new DateDeserializer());

        module.addSerializer(IdGenerator.StringId.class,
                             new IdSerializer<>(IdGenerator.StringId.class));
        module.addSerializer(IdGenerator.LongId.class,
                             new IdSerializer<>(IdGenerator.LongId.class));
        module.addSerializer(EdgeId.class, new IdSerializer<>(EdgeId.class));

        module.addSerializer(HugeVertex.class, new HugeVertexSerializer());
        module.addSerializer(HugeEdge.class, new HugeEdgeSerializer());

        mapper.registerModule(module);
    }

    public static void registerModule(Module module) {
        mapper.registerModule(module);
    }

    public static String toJson(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new BackendException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        E.checkState(json != null,
                     "Json value can't be null for '%s'",
                     clazz.getSimpleName());
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    public static <T> T fromJson(String json, TypeReference<?> typeRef) {
        E.checkState(json != null,
                     "Json value can't be null for '%s'",
                     typeRef.getType());
        try {
            ObjectReader reader = mapper.readerFor(typeRef);
            return reader.readValue(json);
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    /**
     * Number collection will be parsed to Double Collection via fromJson,
     * this method used to cast element in collection to original number type
     * @param object    original number
     * @param clazz     target type
     * @return          target number
     */
    public static Object castNumber(Object object, Class<?> clazz) {
        if (object instanceof Number) {
            Number number = (Number) object;
            if (clazz == Byte.class) {
                object = number.byteValue();
            } else if (clazz == Integer.class) {
                object = number.intValue();
            } else if (clazz == Long.class) {
                object = number.longValue();
            } else if (clazz == Float.class) {
                object = number.floatValue();
            } else {
                assert clazz == Double.class;
            }
        }
        return object;
    }

    private static class DateSerializer extends StdSerializer<Date> {

        private static final long serialVersionUID = -6615155657857746161L;

        public DateSerializer() {
            super(Date.class);
        }

        @Override
        public void serialize(Date date, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeNumber(date.getTime());
        }
    }

    private static class DateDeserializer extends StdDeserializer<Date> {

        private static final long serialVersionUID = 1209944821349424949L;

        public DateDeserializer() {
            super(Date.class);
        }

        @Override
        public Date deserialize(JsonParser parser,
                                DeserializationContext context)
                                throws IOException {
            Long number = parser.readValueAs(Long.class);
            return new Date(number);
        }
    }

    private static class IdSerializer<T extends Id> extends StdSerializer<T> {

        public IdSerializer(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public void serialize(T value, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            if (value.number()) {
                generator.writeNumber(value.asLong());
            } else {
                generator.writeString(value.asString());
            }
        }

        @Override
        public void serializeWithType(T value,
                                      JsonGenerator generator,
                                      SerializerProvider serializers,
                                      TypeSerializer typeSer)
                                      throws IOException {
            typeSer.writeTypePrefixForScalar(value, generator);
            this.serialize(value, generator, serializers);
            typeSer.writeTypeSuffixForScalar(value, generator);
        }
    }

    private static abstract class HugeElementSerializer<T extends HugeElement>
                            extends StdSerializer<T> {

        public HugeElementSerializer(Class<T> clazz) {
            super(clazz);
        }

        public void writeIdField(String fieldName, Id id,
                                 JsonGenerator generator)
                                 throws IOException {
            generator.writeFieldName(fieldName);
            if (id.number()) {
                generator.writeNumber(id.asLong());
            } else {
                generator.writeString(id.asString());
            }
        }

        public void writeProptiesField(Map<Id, HugeProperty<?>> properties,
                                       JsonGenerator generator,
                                       SerializerProvider provider)
                                       throws IOException {
            // Start write properties
            generator.writeFieldName("properties");
            generator.writeStartObject();

            for (HugeProperty<?> property : properties.values()) {
                String key = property.key();
                Object val = property.value();
                try {
                    generator.writeFieldName(key);
                    if (val != null) {
                        JsonSerializer<Object> serializer =
                                provider.findValueSerializer(val.getClass());
                        serializer.serialize(val, generator, provider);
                    } else {
                        generator.writeNull();
                    }
                } catch (IOException e) {
                    throw new HugeException(
                              "Failed to serialize property(%s: %s) " +
                              "for vertex '%s'", key, val, property.element());
                }
            };
            // End wirte properties
            generator.writeEndObject();
        }
    }

    private static class HugeVertexSerializer
                   extends HugeElementSerializer<HugeVertex> {

        public HugeVertexSerializer() {
            super(HugeVertex.class);
        }

        @Override
        public void serialize(HugeVertex vertex, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();

            this.writeIdField("id", vertex.id(), generator);
            generator.writeStringField("label", vertex.label());
            generator.writeStringField("type", "vertex");

            this.writeProptiesField(vertex.getProperties(), generator, provider);
            generator.writeEndObject();
        }
    }

    private static class HugeEdgeSerializer
                   extends HugeElementSerializer<HugeEdge> {

        public HugeEdgeSerializer() {
            super(HugeEdge.class);
        }

        @Override
        public void serialize(HugeEdge edge, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();

            // Write id, label, type
            this.writeIdField("id", edge.id(), generator);
            generator.writeStringField("label", edge.label());
            generator.writeStringField("type", "edge");

            HugeVertex outVertex = (HugeVertex) edge.outVertex();
            HugeVertex inVertex = (HugeVertex) edge.inVertex();
            this.writeIdField("outV", outVertex.id(), generator);
            generator.writeStringField("outVLabel", outVertex.label());
            this.writeIdField("inV", inVertex.id(), generator);
            generator.writeStringField("inVLabel", inVertex.label());

            this.writeProptiesField(edge.getProperties(), generator, provider);

            generator.writeEndObject();
        }
    }
}
