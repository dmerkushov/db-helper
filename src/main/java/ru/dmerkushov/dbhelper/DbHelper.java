/*
 * Copyright 2013 dmerkushov.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.dmerkushov.dbhelper;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import ru.dmerkushov.loghelper.LoggerWrapper;

/**
 *
 * @author Dmitriy Merkushov
 */
public class DbHelper {

	Connection dbConnection = null;
	String driverName = null;
	String connectionUrl = null;
	static LoggerWrapper loggerWrapper = null;

	/**
	 *
	 * @param driverName JDBC driver class name (i.e.,
	 * "com.informix.jdbc.IfxDriver")
	 * @param connectionUrl
	 */
	public DbHelper (String driverName, String connectionUrl) {
		getLoggerWrapper ().entering (driverName, connectionUrl);

		this.driverName = driverName;
		this.connectionUrl = connectionUrl;

		getLoggerWrapper ().exiting ();
	}
	
	/**
	 * Get the {@link LoggerWrapper} instance for DbHelper
	 * @return 
	 */
	public static LoggerWrapper getLoggerWrapper () {
		if (loggerWrapper == null) {
			loggerWrapper = LoggerWrapper.getLoggerWrapper ("ru.dmerkushov.dbhelper.DbHelper");
			loggerWrapper.configureByDefaultDailyRolling ("log/DbHelper_%d_%u.log");
		}
		
		return loggerWrapper;
	}

	/**
	 * Perform a query to the database
	 *
	 * @param sql
	 * @return
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public ResultSet performDbQuery (String sql) throws DbHelperException {
		getLoggerWrapper ().entering (sql);

		ResultSet resultSet = performDbQuery (sql, null);

		getLoggerWrapper ().exiting (resultSet);

		return resultSet;
	}

	/**
	 * Perform a query to the database
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Query parameters. Supported types are: {@link String}, {@link Boolean}, {@link Long}, {@link Integer}, {@link Double}, {@link Float}, {@link java.sql.Time}, {@link java.sql.Timestamp}, {@link java.sql.Date}
	 * @return
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 * @throws java.lang.IllegalArgumentException If one or more of the params is not of supported type
	 */
	public ResultSet performDbQuery (String sql, Object[] sqlParams) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams);

		ResultSet toReturn = null;
		PreparedStatement ps = null;

		openDbConnection ();

		if (dbConnection == null) {
			throw new DbHelperException ("Database connection");
		}
		if (sql == null) {
			throw new DbHelperException ("SQL provided is null");
		}

		if (dbConnection != null) {
			getLoggerWrapper ().info ("Preparing a statement for SQL: \"" + sql + "\"");
			try {
				ps = dbConnection.prepareStatement (sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to prepare statement for SQL: \"" + sql + "\".", ex);
			}

			getLoggerWrapper ().info ("PreparedStatement for SQL: \"" + sql + "\" prepared");

			try {
				if (sqlParams != null) {

					getLoggerWrapper ().info ("Running through parameters for SQL: \"" + sql + "\"");

					for (int paramIndex = 0; paramIndex < sqlParams.length; paramIndex++) {
						Object param = sqlParams[paramIndex];

						if (param == null) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is null");
							try {
								ps.setObject (paramIndex + 1, null);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a null parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.String) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a String: " + (String) param);
							try {
								ps.setString (paramIndex + 1, (String) param);					// i+1, because the first parameter for PreparedStatement.setX() functions is #1
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a String parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Boolean) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Boolean: " + String.valueOf (param));

							try {
								if (ps.getConnection ().getMetaData ().getDriverName ().contains ("Informix")) {	// Informix JDBC driver has no direct support for setBoolean()
									getLoggerWrapper ().info ("Database type is Informix, must use strings \"T\"/\"F\" for boolean");
									ps.setString (paramIndex + 1, (Boolean) param ? "t" : "f");
								} else {
									ps.setBoolean (paramIndex + 1, (Boolean) param);
								}
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Boolean parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Long) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Long: " + String.valueOf (param));

							try {
								ps.setLong (paramIndex + 1, (Long) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Long parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Integer) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is an Integer: " + String.valueOf (param));

							try {
								ps.setInt (paramIndex + 1, (Integer) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set an Integer parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Double) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Double: " + String.valueOf (param));

							try {
								ps.setDouble (paramIndex + 1, (Double) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Double parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Float) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Float: " + String.valueOf (param));

							try {
								ps.setFloat (paramIndex + 1, (Float) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Float parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Timestamp) {

							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Timestamp: " + sdf.format ((java.sql.Timestamp) param));

							try {
								ps.setTimestamp (paramIndex + 1, (java.sql.Timestamp) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Time) {

							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Time: " + sdf.format ((java.sql.Time) param));

							try {
								ps.setTime (paramIndex + 1, (java.sql.Time) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Date) {

							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Date: " + sdf.format ((java.sql.Date) param));

							try {
								ps.setDate (paramIndex + 1, (java.sql.Date) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else {
							IllegalArgumentException iae = new IllegalArgumentException ("Illegal class of parameter #" + String.valueOf (paramIndex).trim () + ": " + (param != null ? param.getClass ().getName () : "null") + ".\n SQL is \"" + sql + "\".\n Supported classes are: String, Boolean, Long, Integer, Double, Float, and java.sql.Date");
							throw iae;
						}
					}
				}

				getLoggerWrapper ().info ("Executing query for SQL: \"" + sql + "\"");

				try {
					toReturn = ps.executeQuery ();
				} catch (SQLException ex) {
					throw new DbHelperException ("Received a SQLException when trying to execute query for SQL: \"" + sql + "\".", ex);
				}

			} finally {
			}

		}

		getLoggerWrapper ().exiting (toReturn);

		return toReturn;
	}

	/**
	 * <p>
	 * Get the value of the designated column in the first row
	 * of the <code>ResultSet</code> object denoted by the query and parameters as
	 * an <code>Object</code> in the Java programming language.
	 *
	 * <p>
	 * This method will return the value of the given column as a
	 * Java object. The type of the Java object will be the default
	 * Java object type corresponding to the column's SQL type,
	 * following the mapping for built-in types specified in the JDBC
	 * specification. If the value is an SQL <code>NULL</code>,
	 * the driver returns a Java <code>null</code>.
	 * <P>
	 * This method may also be used to read database-specific
	 * abstract data types.
	 * <P>
	 * In the JDBC 2.0 API, the behavior of the method
	 * <code>getObject</code> is extended to materialize
	 * data of SQL user-defined types. When a column contains
	 * a structured or distinct value, the behavior of this method is as
	 * if it were a call to: <code>getObject(columnIndex,
	 * this.getStatement().getConnection().getTypeMap())</code>.
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String, Boolean,
	 * Long, Integer, Double, Float, and java.sql.Date
	 * @param columnLabel the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then the label is the name of the column
	 * @return an {@link java.lang.Object} holding the column value, or null if the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public Object performDbQuerySingleResult (String sql, Object[] sqlParams, String columnLabel) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams, columnLabel);

		ResultSet rs = this.performDbQuery (sql, sqlParams);

		boolean success;
		try {
			success = rs.next ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		Object result = null;
		if (success) {
			try {
				result = rs.getObject (columnLabel);
			} catch (SQLException ex) {
				throw new DbHelperException (ex);
			}
		}

		getLoggerWrapper ().exiting (result);
		return result;
	}

	/**
	 * <p>
	 * Get the value of the designated column in the first row
	 * of the <code>ResultSet</code> object denoted by the query and parameters as
	 * an <code>Object</code> in the Java programming language.
	 *
	 * <p>
	 * This method will return the value of the given column as a
	 * Java object. The type of the Java object will be the default
	 * Java object type corresponding to the column's SQL type,
	 * following the mapping for built-in types specified in the JDBC
	 * specification. If the value is an SQL <code>NULL</code>,
	 * the driver returns a Java <code>null</code>.
	 * <P>
	 * This method may also be used to read database-specific
	 * abstract data types.
	 * <P>
	 * In the JDBC 2.0 API, the behavior of the method
	 * <code>getObject</code> is extended to materialize
	 * data of SQL user-defined types. When a column contains
	 * a structured or distinct value, the behavior of this method is as
	 * if it were a call to: <code>getObject(columnIndex,
	 * this.getStatement().getConnection().getTypeMap())</code>.
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String, Boolean,
	 * Long, Integer, Double, Float, and java.sql.Date
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a <code>java.lang.Object</code> holding the column value, or null if the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public Object performDbQuerySingleResult (String sql, Object[] sqlParams, int columnIndex) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams, columnIndex);

		ResultSet rs = this.performDbQuery (sql, sqlParams);

		boolean success;
		try {
			success = rs.next ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		Object result = null;
		if (success) {
			try {
				result = rs.getObject (columnIndex);
			} catch (SQLException ex) {
				throw new DbHelperException (ex);
			}
		}

		getLoggerWrapper ().exiting (result);
		return result;
	}

	/**
	 * Perform a query with a single result checking its type
	 *
	 * @param sql
	 * @param sqlParams
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param clazz the class of which the result must be an instance, else an exception will be thrown
	 * @return an {@link java.lang.Object} that can be converted to the given class, or null if the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException 1. Any underlying exception<br/>
	 * 2. When the result is not an instance of the class specified, the message will begin with: "Wrong result type", and after that the specification of the result type and what was expected
	 */
	public Object performDbQuerySingleResultCheckType (String sql, Object[] sqlParams, int columnIndex, Class clazz) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams, columnIndex, clazz);

		Object result = performDbQuerySingleResult (sql, sqlParams, columnIndex);

		if (result != null && !clazz.isInstance (result)) {
			throw new DbHelperException ("Wrong result type: result is " + result.getClass ().getCanonicalName () + ", expected " + clazz.getCanonicalName ());
		}

		return result;
	}

	/**
	 * Perform a query with a single result checking its type
	 *
	 * @param sql
	 * @param sqlParams
	 * @param columnLabel
	 * @param clazz the class of which the result must be an instance, else an exception will be thrown
	 * @return an {@link java.lang.Object} that can be converted to the given class, or null if the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException 1. Any underlying exception<br/>
	 * 2. When the result is not an instance of the class specified, the message will begin with: "Wrong result type", and after that the specification of the result type and what was expected
	 */
	public Object performDbQuerySingleResultCheckType (String sql, Object[] sqlParams, String columnLabel, Class clazz) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams, columnLabel, clazz);

		Object result = performDbQuerySingleResult (sql, sqlParams, columnLabel);

		if (result != null && !clazz.isInstance (result)) {
			throw new DbHelperException ("Wrong result type: result is " + result.getClass ().getCanonicalName () + ", expected " + clazz.getCanonicalName ());
		}

		return result;
	}

	/**
	 * Perform a query and get a single column as a list
	 * @param sql
	 * @param sqlParams
	 * @param columnLabel
	 * @return List of results, or an empty list (not null) when there were no results
	 * @throws ru.dmerkushov.dbhelper.DbHelperException 
	 */
	public List<Object> performDbQueryList (String sql, Object[] sqlParams, String columnLabel) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams, columnLabel);

		List<Object> result = new ArrayList<> ();

		ResultSet rs = this.performDbQuery (sql, sqlParams);

		boolean hasNext;
		try {
			hasNext = rs.next ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		while (hasNext) {
			try {
				result.add (rs.getObject (columnLabel));
			} catch (SQLException ex) {
				throw new DbHelperException (ex);
			}
			try {
				hasNext = rs.next ();
			} catch (SQLException ex) {
				throw new DbHelperException (ex);
			}
		}

		getLoggerWrapper ().exiting (result);
		return result;
	}

	/**
	 * Perform a query and get a single column as a list
	 * @param sql
	 * @param sqlParams
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return List of results, or an empty list (not null) when there were no results
	 * @throws ru.dmerkushov.dbhelper.DbHelperException 
	 */
	public List<Object> performDbQueryList (String sql, Object[] sqlParams, int columnIndex) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams, columnIndex);

		List<Object> result = new ArrayList<> ();

		ResultSet rs = this.performDbQuery (sql, sqlParams);

		boolean hasNext;
		try {
			hasNext = rs.next ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		while (hasNext) {
			try {
				result.add (rs.getObject (columnIndex));
			} catch (SQLException ex) {
				throw new DbHelperException (ex);
			}
			try {
				hasNext = rs.next ();
			} catch (SQLException ex) {
				throw new DbHelperException (ex);
			}
		}

		getLoggerWrapper ().exiting (result);
		return result;
	}

	/**
	 * Check if a record exists
	 *
	 * @param sql
	 * @param params
	 * @return
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 * @throws SQLException
	 */
	public boolean recordExists (String sql, Object[] params) throws DbHelperException, SQLException {
		getLoggerWrapper ().entering (sql, params);

		boolean exists;
		try (ResultSet existsRs = performDbQuery (sql, params)) {
			exists = existsRs.next ();
		}

		getLoggerWrapper ().exiting (exists);
		return exists;
	}

	/**
	 * Perform an update to the database
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Query parameters. Supported types are: {@link String}, {@link Boolean}, {@link Long}, {@link Integer}, {@link Double}, {@link Float}, {@link java.sql.Time}, {@link java.sql.Timestamp}, {@link java.sql.Date}
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 * @throws IllegalArgumentException If one or more of the params is not of supported class
	 */
	public int performDbUpdate (String sql, Object... sqlParams) throws DbHelperException {
		getLoggerWrapper ().entering (sql, sqlParams);

		Integer toReturn = null;
		PreparedStatement ps = null;

		openDbConnection ();

		this.performDbUpdate (sql, sqlParams[0], sqlParams[2]);

		if (dbConnection == null) {
			throw new DbHelperException ("Database connection is null");
		}
		if (sql == null) {
			throw new DbHelperException ("SQL provided is null");
		}
		if (sql.equals ("")) {
			throw new DbHelperException ("SQL provided is empty");
		}

		if (dbConnection != null) {
			getLoggerWrapper ().info ("Preparing a statement for SQL: \"" + sql + "\"");
			try {
				ps = dbConnection.prepareStatement (sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to prepare statement for SQL: \"" + sql + "\".", ex);
			}

			getLoggerWrapper ().info ("PreparedStatement for SQL: \"" + sql + "\" prepared");

			try {
				if (sqlParams != null) {

					getLoggerWrapper ().info ("Running through parameters for SQL: \"" + sql + "\"");

					for (int paramIndex = 0; paramIndex < sqlParams.length; paramIndex++) {
						Object param = sqlParams[paramIndex];

						if (param == null) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a null");
							try {
								ps.setObject (paramIndex + 1, null);					// i+1, because the first parameter for PreparedStatement.setX() functions is #1
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a String parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.String) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a String: " + (String) param);
							try {
								ps.setString (paramIndex + 1, (String) param);					// i+1, because the first parameter for PreparedStatement.setX() functions is #1
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a String parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Boolean) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Boolean: " + String.valueOf (param));

							try {
								if (ps.getConnection ().getMetaData ().getDriverName ().contains ("Informix")) {	// Informix JDBC driver has no direct support for setBoolean()
									getLoggerWrapper ().info ("Database type is Informix, must use strings \"T\"/\"F\" for boolean");
									ps.setString (paramIndex + 1, (Boolean) param ? "t" : "f");
								} else {
									ps.setBoolean (paramIndex + 1, (Boolean) param);
								}
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Boolean parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Long) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Long: " + String.valueOf (param));

							try {
								ps.setLong (paramIndex + 1, (Long) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Long parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Integer) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is an Integer: " + String.valueOf (param));

							try {
								ps.setInt (paramIndex + 1, (Integer) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set an Integer parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Double) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Double: " + String.valueOf (param));

							try {
								ps.setDouble (paramIndex + 1, (Double) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Double parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Float) {

							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Float: " + String.valueOf (param));

							try {
								ps.setFloat (paramIndex + 1, (Float) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Float parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Timestamp) {

							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Timestamp: " + sdf.format ((java.sql.Timestamp) param));

							try {
								ps.setTimestamp (paramIndex + 1, (java.sql.Timestamp) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Time) {

							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Time: " + sdf.format ((java.sql.Time) param));

							try {
								ps.setTime (paramIndex + 1, (java.sql.Time) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Date) {

							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							getLoggerWrapper ().info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Date: " + sdf.format ((java.sql.Date) param));

							try {
								ps.setDate (paramIndex + 1, (java.sql.Date) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else {
							IllegalArgumentException iae = new IllegalArgumentException ("Illegal class of parameter #" + String.valueOf (paramIndex).trim () + ": " + (param != null ? param.getClass ().getName () : "null") + ".\n SQL is \"" + sql + "\".\n Supported classes are: String, Boolean, Long, Integer, Double, Float, java.sql.Date, java.sql.Time, and java.sql.Timestamp");
							throw iae;
						}
					}
				}

				getLoggerWrapper ().info ("Executing update for SQL: \"" + sql + "\"");

				try {
					toReturn = ps.executeUpdate ();
				} catch (SQLException ex) {
					throw new DbHelperException ("Received a SQLException when trying to execute query for SQL: \"" + sql + "\".", ex);
				}

			} finally {
			}

		}

		getLoggerWrapper ().exiting (toReturn);

		return toReturn;
	}

	/**
	 * Create a DB connection
	 *
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void openDbConnection () throws DbHelperException {
		getLoggerWrapper ().entering ();

		if (driverName == null) {
			throw new DbHelperException ("driverName supplied is null");
		}
		if (driverName.equals ("")) {
			throw new DbHelperException ("driverName supplied is empty (not null)");
		}

		/**
		 * A flag to indicate whether really do open a new connection
		 */
		boolean doOpenDbConnection = false;

		if (dbConnection == null) {
			doOpenDbConnection = true;
		} else {
			/*
			 * try { if (!dbConnection.isValid (10)) { doOpenDbConnection =
			 * true; } } catch (SQLException ex) { throw new DbHelperException
			 * ("Received a SQLException when trying to check whether the
			 * connection is valid.", ex);
			 * }
			 */
		}

		if (doOpenDbConnection) {
			getLoggerWrapper ().info ("Need to open a connection");

			try {
				Class.forName (driverName);
			} catch (ClassNotFoundException ex) {
				throw new DbHelperException ("Received a ClassNotFoundException when trying to initialize a class for the database driver: " + driverName, ex);
			}

			getLoggerWrapper ().info ("Found class for driver name: " + driverName);

			try {
				dbConnection = DriverManager.getConnection (connectionUrl);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to get a connection.", ex);
			}

			getLoggerWrapper ().info ("Got a connection from the driver");

			try {
				dbConnection.setAutoCommit (true);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to set autocommit on", ex);
			}

			getLoggerWrapper ().info ("Autocommit is set to true");

			if (driverName.equals ("com.informix.jdbc.IfxDriver")) {
				getLoggerWrapper ().info ("Informix driver. Need to set DIRTY READ mode");
				Statement stmt = null;
				try {
					stmt = dbConnection.createStatement ();
				} catch (SQLException ex) {
					throw new DbHelperException (ex);
				}
				try {
					stmt.execute ("set isolation to dirty read");
				} catch (SQLException ex) {
					throw new DbHelperException (ex);
				}
				getLoggerWrapper ().info ("DIRTY READ mode set");
			}
		} else {
			getLoggerWrapper ().info ("Do not need to open a connection");
		}

		getLoggerWrapper ().exiting ();
	}

	/**
	 * Set the auto-commit state of the connection
	 *
	 * @param autoCommit
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void setAutoCommit (boolean autoCommit) throws DbHelperException {
		getLoggerWrapper ().entering (autoCommit);

		openDbConnection ();

		try {
			dbConnection.setAutoCommit (autoCommit);
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to set autocommit to " + autoCommit + ".", ex);
		}

		getLoggerWrapper ().exiting ();
	}

	/**
	 * Get the auto-commit state of the connection
	 *
	 * @return
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public boolean getAutoCommit () throws DbHelperException {
		getLoggerWrapper ().entering ();

		openDbConnection ();

		boolean autoCommit;
		try {
			autoCommit = dbConnection.getAutoCommit ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to get autocommit for the connection.", ex);
		}

		getLoggerWrapper ().exiting (autoCommit);
		return autoCommit;
	}

	/**
	 * Commit the current transaction
	 *
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void commit () throws DbHelperException {
		getLoggerWrapper ().entering ();

		openDbConnection ();

		try {
			dbConnection.commit ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to commit transaction.", ex);
		}

		getLoggerWrapper ().exiting ();
	}

	/**
	 * Rollback the current transaction
	 *
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void rollback () throws DbHelperException {
		getLoggerWrapper ().entering ();

		openDbConnection ();

		try {
			dbConnection.rollback ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to rollback transaction.", ex);
		}

		getLoggerWrapper ().exiting ();
	}

	/**
	 * Release the connection
	 *
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public synchronized void releaseConnection () throws DbHelperException {
		getLoggerWrapper ().entering ();

		if (dbConnection != null) {
			try {
				dbConnection.close ();
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to close the connection.", ex);
			}
			dbConnection = null;
		}

		getLoggerWrapper ().exiting ();
	}

	@Override
	protected void finalize () throws Throwable {
		super.finalize ();
		this.releaseConnection ();
	}
}
