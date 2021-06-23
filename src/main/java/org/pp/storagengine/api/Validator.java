package org.pp.storagengine.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Validator {
	/** if value is power of two */
	public boolean powof2() default false;	
	/** Negative value is not allowed by default */
	public boolean negtv() default false;
	/** Minimum integer value, not enabled by default */
	public long min() default -1;
	/** Maximum Integer value, not enabled by default*/
	public long max() default -1;
	/** For String value */
	public String regex() default "";
	/** By default filed value is not required */
	public boolean reqrd() default false;	
}
