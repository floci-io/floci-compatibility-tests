package com.floci.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@link TestGroup} implementation for automatic discovery.
 *
 * <p>Classes annotated with {@code @FlociTestGroup} are collected at compile time by
 * {@link FlociTestGroupProcessor} and registered in the generated
 * {@code TestGroupRegistry} class. {@link FlociTest} loads all groups from that
 * registry instead of maintaining a manual list.
 *
 * <p>Example:
 * <pre>
 *   &#64;FlociTestGroup
 *   public class SqsTests implements TestGroup { ... }
 * </pre>
 *
 * <p>Use {@link #value()} (or the named alias {@link #order()}) to control execution
 * order — lower values run first. Groups with the same value sort alphabetically.
 * Both forms are equivalent:
 * <pre>
 *   &#64;FlociTestGroup(2)          // shorthand
 *   &#64;FlociTestGroup(order = 2)  // named
 * </pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface FlociTestGroup {

    /**
     * Shorthand for {@link #order()}: {@code @FlociTestGroup(2)} sets the execution order to 2.
     * Ignored when {@link #order()} is set explicitly.
     */
    int value() default 0;

    /**
     * Execution order relative to other groups. Lower values run first.
     * Groups with the same value are sorted alphabetically by class name.
     * Use {@code -1} (the default sentinel) to fall back to {@link #value()}.
     */
    int order() default -1;
}
