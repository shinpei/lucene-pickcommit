package com.github.shinpei.segmerge;

import org.immutables.value.Value;

@Value.Immutable
abstract class Config {

    @Value.Parameter
    abstract int deleteIndex();

}
