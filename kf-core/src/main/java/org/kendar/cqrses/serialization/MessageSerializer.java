package org.kendar.cqrses.serialization;

public interface MessageSerializer<K,J> {
    byte[] serialize(Object domainObject) ;

    <T> T deserialize(byte[] payload, Class<T> targetClass) ;


    K  serializeToFormat(Object domainObject) ;

    <T> T deserializeFromFormat(K payload, Class<T> targetClass) ;

    K deserializeToFormat(byte[] payload) ;
    J deserializeToIntermediate(byte[] payload) ;
}
