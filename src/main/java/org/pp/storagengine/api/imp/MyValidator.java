package org.pp.storagengine.api.imp;

import static org.pp.storagengine.api.imp.Util.GB;
import static org.pp.storagengine.api.imp.Util.MB;
import static org.pp.storagengine.api.imp.Util.extrNum;
import static org.pp.storagengine.api.imp.Util.runExcp;
import static org.pp.storagengine.api.imp.Util.validateNum;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.pp.storagengine.api.Validator;


public class MyValidator {
    @Validator(min=MB, powof2=true, reqrd=true)
    private int bSiz = 4 * 1024;
    
    @Validator(min=GB, max=16 * GB, powof2=true, reqrd=true)
    private long dBSize = 16 * (long) GB;
    
    @Validator(reqrd=true, regex="^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$")
    private String email = "";
    
    public static void main(String[] args) throws Exception { 
		Properties props = System.getProperties();
		props.setProperty("bSiz", "4M");
		props.setProperty("dBSize", "8G");
		props.setProperty("email", "pan.prasanta@gmail.com");
		props.setProperty("rFile", "T");
		MyValidator mVald = new MyValidator();
		validateProps(mVald, props);		 
	}	
	// Validate against map
	static final void validateMap(Object target, Map<String, String> map) {
		validate(target,map, target.getClass());		
	}
	// Validate against properties 
	static final void validateProps(Object target, Properties props) {
		validate(target,props,target.getClass());
	}
	static final void validateProps(Class<?> classz, Properties props) {
		validate(null,props,classz);	
	}
	// Save all annotated variables to properties file
	static final Properties toProperties(Object target) throws Exception {
		Properties props = new Properties();
		Class<?> clazz = target.getClass();			
		for (Field field : clazz.getDeclaredFields()) {
			if (field.isAnnotationPresent(Validator.class)) {
				field.setAccessible(true);
				props.setProperty(field.getName(),field.get(target).toString());				
			}
		}
		return props;
	}
	// map properties back to object field
	static final void propToObj(Object target, Properties props) throws Exception {
		Class<?> clazz = target.getClass();
		String sVal = null; Object val = null;
		for (Field field : clazz.getDeclaredFields()) {
			if (field.isAnnotationPresent(Validator.class)) {
				field.setAccessible(true);
				sVal = props.getProperty(field.getName());
				if (field.getType() == Integer.TYPE) 
					val = Integer.parseInt(sVal);
				else if (field.getType() == Long.TYPE)
					val = Long.parseLong(sVal);
				else if (field.getType() == Boolean.TYPE)
					val = Boolean.parseBoolean(sVal);
				else 
					val = sVal;
				// Set value
				field.set(target, val);
			}
		}		
	}
	// Common Validator for both Map and properties
	private static final void validate(Object target, @SuppressWarnings("rawtypes") Map map, Class<?> clazz) {
		if (map == null || map.isEmpty()) return;
		String fldName = null, val = null;
		Object oVal = null; Validator validator = null;
		for (Field field : clazz.getDeclaredFields()) {
			// just continue If the field is not annotated 
			if (!field.isAnnotationPresent(Validator.class)) continue;
			// make the field accessible first
			field.setAccessible(true);
			// get the field name
			fldName = field.getName();			
			// Get VALIDATOR annotation present in the field
			validator = field.getAnnotation(Validator.class);
			// Retrieve the corresponding value from properties
			val = (String) map.get(fldName);
			// if the field is required but not present in map
			if (val == null || "".equals(val = val.trim())) val = "";
			if (validator.reqrd() && "".equals(val))	
				runExcp("The required field '" + fldName + "' is not present in map or properties");
			// continue if no value
			if ("".equals(val)) continue;
			// Validate LONG type value
			if (field.getType() == Long.TYPE) 
			   oVal = validateNum(extrNum(val,fldName), validator, fldName);
			else if (field.getType() == Integer.TYPE)
			   oVal = (int) validateNum(extrNum(val,fldName), validator, fldName);
			// validate String type value
			else if (field.getType() == String.class && !"".equals(validator.regex().trim())) {
				Pattern pattn = Pattern.compile(validator.regex().trim(),Pattern.CASE_INSENSITIVE);
				Matcher matcher = pattn.matcher(val);
				if (!matcher.matches())
				   runExcp("Pattern matching failed for the field '" + fldName + "'");	
				oVal = val;									
			}
			else if (field.getType() == String.class) 
				oVal = val;
			else if (field.getType() == boolean.class)
				oVal = Boolean.parseBoolean(val);
			else 
				runExcp("Field '" + fldName + "' data type is not supported");
			// Set value to field
			try { field.set(target, oVal); } catch (Exception e) { throw new RuntimeException(e); };  
	    }	
	}	
	
}
