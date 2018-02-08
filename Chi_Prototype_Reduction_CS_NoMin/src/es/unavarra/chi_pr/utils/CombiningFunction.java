/*
 * Author: Alpine Data Labs
 * Link: https://alpinenow.com/blog/in-mapper-combiner/
 */

package es.unavarra.chi_pr.utils;

import org.apache.hadoop.io.Writable;

public interface CombiningFunction<VALUE extends Writable> {
    public VALUE combine(VALUE value1, VALUE value2);
}
