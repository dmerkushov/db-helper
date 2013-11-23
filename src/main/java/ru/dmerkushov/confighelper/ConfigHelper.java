/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.dmerkushov.confighelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 * @author shandr
 */
public class ConfigHelper {

	private static HashMap<String, Properties> propertiesMap = new HashMap<String, Properties> ();

	/**
	 * Get a configuration from the centralized storage by its name
	 *
	 * @param name
	 * @return if the name is new, return an empty configuration
	 */
	public static Properties getConfig (String name) {
		if (name == null) {
			name = "auto_" + Long.toString (Calendar.getInstance ().getTimeInMillis ());
		}

		if (!configExists (name)) {
			createConfig (name);
		}
		Properties config = propertiesMap.get (name);
		return config;
	}

	/**
	 * Get a configuration from the centralized storage by its name, then fill
	 * it from a file
	 *
	 * @param name
	 * @param filename
	 * @return
	 * @throws ConfigHelperException if there is an error finding the file or
	 * reading from it
	 */
	public static Properties getConfig (String name, String filename) throws ConfigHelperException {
		Properties config = getConfig (name, new Properties (), filename);

		return config;
	}

	/**
	 * Get a configuration from the centralized storage by its name, fill it
	 * with the given default values, then fill it from a file (only if the file
	 * exists and can be read)
	 *
	 * @param defaultValues
	 * @param name
	 * @param filename
	 * @return
	 * @throws ConfigHelperException
	 */
	public static Properties getConfig (String name, Properties defaultValues, String filename) throws ConfigHelperException {
		Properties config = getConfig (name);

		if (defaultValues != null) {
			for (String key : defaultValues.stringPropertyNames ()) {
				config.setProperty (key, defaultValues.getProperty (key));
			}
		}

		if (filename != null) {
			File propertiesFile = new File (filename);
			if (propertiesFile.exists () && propertiesFile.canRead ()) {
				FileInputStream propertiesFis;
				try {
					propertiesFis = new FileInputStream (propertiesFile);
				} catch (FileNotFoundException ex) {
					throw new ConfigHelperException (ex);
				}
				try {
					config.load (propertiesFis);
				} catch (IOException ex) {
					throw new ConfigHelperException (ex);
				}
			}
		}

		return config;
	}

	/**
	 * Get a configuration from the centralized storage by its name, then fill
	 * it with the given default values
	 *
	 * @param defaultValues
	 * @param name
	 * @param filename
	 * @return
	 * @throws ConfigHelperException
	 */
	public static Properties getConfig (String name, Properties defaultValues) throws ConfigHelperException {
		Properties config = getConfig (name);

		for (String key : defaultValues.stringPropertyNames ()) {
			config.setProperty (key, defaultValues.getProperty (key));
		}

		return config;
	}

	/**
	 * Create a new configuration (or erase the existing one)
	 *
	 * @param name
	 */
	public static void createConfig (String name) {
		Properties toCreate = new Properties ();
		propertiesMap.put (name, toCreate);
	}

	/**
	 * Create a new configuration (or erase the existing one) with the specified
	 * values
	 *
	 * @param name
	 * @param values
	 * @throws ConfigHelperException
	 */
	public static void createConfig (String name, Properties values) throws ConfigHelperException {
		createConfig (name);
		getConfig (name, values);
	}

	/**
	 * Create a new configuration (or erase the existing one) with the specified
	 * values, then fill it from a file
	 *
	 * @param values
	 * @param name
	 * @param filename
	 * @throws ConfigHelperException
	 */
	public static void createConfig (String name, Properties values, String filename) throws ConfigHelperException {
		createConfig (name);
		getConfig (name, values, filename);
	}

	/**
	 * Remove a configuration
	 *
	 * @param name
	 */
	public static void removeConfig (String name) {
		propertiesMap.remove (name);
	}

	/**
	 * Does a configuration exist?
	 *
	 * @param name
	 * @return
	 */
	public static boolean configExists (String name) {
		return propertiesMap.containsKey (name);
	}
}
