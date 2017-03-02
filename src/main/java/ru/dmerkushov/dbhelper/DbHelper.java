/*
 * Copyright 2013-2017 dmerkushov.
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import ru.dmerkushov.loghelper.LoggerWrapper;

/**
 *
 * @author Dmitriy Merkushov
 */
public class DbHelper {

	Connection dbConnection = null;
	String driverName = null;
	String connectionUrl = null;
	String username = null;
	String password;

	public static final LoggerWrapper loggerWrapper;

	static {
		loggerWrapper = LoggerWrapper.getLoggerWrapper ("DbHelper");
		loggerWrapper.configureByDefault (null);
	}

	/**
	 *
	 * @param driverName JDBC driver class name (i.e.,
	 * "com.informix.jdbc.IfxDriver")
	 * @param connectionUrl
	 */
	public DbHelper (String driverName, String connectionUrl) {
		this (driverName, connectionUrl, null, null);
	}

	public DbHelper (String driverName, String connectionUrl, String username, String password) {
		loggerWrapper.entering (driverName, connectionUrl, username, password);

		Objects.requireNonNull (driverName, "driverName");
		Objects.requireNonNull (connectionUrl, "connectionUrl");

		this.driverName = driverName;
		this.connectionUrl = connectionUrl;
		this.username = username;
		this.password = password;

		loggerWrapper.exiting ();
	}

	/**
	 * Perform a query to the database
	 *
	 * @param sql
	 * @return
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public ResultSet performDbQuery (String sql) throws DbHelperException {
		loggerWrapper.entering (sql);

		ResultSet resultSet = performDbQuery (sql, null);

		loggerWrapper.exiting (resultSet);

		return resultSet;
	}

	/**
	 * Perform a query to the database
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Query parameters. Supported types are:
	 * {@link String}, {@link Boolean}, {@link Long}, {@link Integer}, {@link Double}, {@link Float}, {@link java.sql.Time}, {@link java.sql.Timestamp}, {@link java.sql.Date},
	 * byte[]
	 * @return
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 * @throws java.lang.IllegalArgumentException If one or more of the params
	 * is not of supported type
	 */
	public ResultSet performDbQuery (String sql, Object[] sqlParams) throws DbHelperException {
		Objects.requireNonNull (sql, "sql");
		if (sql.equals ("")) {
			throw new IllegalArgumentException ("SQL provided is empty");
		}

		loggerWrapper.entering (sql, sqlParams);

		ResultSet toReturn = null;
		PreparedStatement ps;
		try {
			ps = prepareStatement (sql, sqlParams);
		} catch (DbHelperException ex) {
			throw new DbHelperException ("Received a DbHelperException when trying to prepare statement for SQL: \"" + sql + "\".", ex);
		}

		try {
			toReturn = ps.executeQuery ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to execute query for SQL: \"" + sql + "\".", ex);
		}

		loggerWrapper.exiting (toReturn);

		return toReturn;
	}

	/**
	 * <p>
	 * Get the value of the designated column in the first row of the
	 * <code>ResultSet</code> object denoted by the query and parameters as an
	 * <code>Object</code> in the Java programming language.
	 *
	 * <p>
	 * This method will return the value of the given column as a Java object.
	 * The type of the Java object will be the default Java object type
	 * corresponding to the column's SQL type, following the mapping for
	 * built-in types specified in the JDBC specification. If the value is an
	 * SQL <code>NULL</code>, the driver returns a Java <code>null</code>.
	 * <P>
	 * This method may also be used to read database-specific abstract data
	 * types.
	 * <P>
	 * In the JDBC 2.0 API, the behavior of the method <code>getObject</code> is
	 * extended to materialize data of SQL user-defined types. When a column
	 * contains a structured or distinct value, the behavior of this method is
	 * as if it were a call to: <code>getObject(columnIndex,
	 * this.getStatement().getConnection().getTypeMap())</code>.
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String,
	 * Boolean, Long, Integer, Double, Float, and java.sql.Date
	 * @param columnLabel the label for the column specified with the SQL AS
	 * clause. If the SQL AS clause was not specified, then the label is the
	 * name of the column
	 * @return an {@link java.lang.Object} holding the column value, or null if
	 * the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public Object performDbQuerySingleResult (String sql, Object[] sqlParams, String columnLabel) throws DbHelperException {
		loggerWrapper.entering (sql, sqlParams, columnLabel);

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

		loggerWrapper.exiting (result);
		return result;
	}

	/**
	 * <p>
	 * Get the value of the designated column in the first row of the
	 * <code>ResultSet</code> object denoted by the query and parameters as an
	 * <code>Object</code> in the Java programming language.
	 *
	 * <p>
	 * This method will return the value of the given column as a Java object.
	 * The type of the Java object will be the default Java object type
	 * corresponding to the column's SQL type, following the mapping for
	 * built-in types specified in the JDBC specification. If the value is an
	 * SQL <code>NULL</code>, the driver returns a Java <code>null</code>.
	 * <P>
	 * This method may also be used to read database-specific abstract data
	 * types.
	 * <P>
	 * In the JDBC 2.0 API, the behavior of the method <code>getObject</code> is
	 * extended to materialize data of SQL user-defined types. When a column
	 * contains a structured or distinct value, the behavior of this method is
	 * as if it were a call to: <code>getObject(columnIndex,
	 * this.getStatement().getConnection().getTypeMap())</code>.
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String,
	 * Boolean, Long, Integer, Double, Float, and java.sql.Date
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return a <code>java.lang.Object</code> holding the column value, or null
	 * if the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public Object performDbQuerySingleResult (String sql, Object[] sqlParams, int columnIndex) throws DbHelperException {
		loggerWrapper.entering (sql, sqlParams, columnIndex);

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

		loggerWrapper.exiting (result);
		return result;
	}

	/**
	 * Perform a query with a single result checking its type
	 *
	 * @param sql
	 * @param sqlParams
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @param clazz the class of which the result must be an instance, else an
	 * exception will be thrown
	 * @return an {@link java.lang.Object} that can be converted to the given
	 * class, or null if the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException 1. Any underlying
	 * exception<br>
	 * 2. When the result is not an instance of the class specified, the message
	 * will begin with: "Wrong result type", and after that the specification of
	 * the result type and what was expected
	 */
	public Object performDbQuerySingleResultCheckType (String sql, Object[] sqlParams, int columnIndex, Class clazz) throws DbHelperException {
		loggerWrapper.entering (sql, sqlParams, columnIndex, clazz);

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
	 * @param clazz the class of which the result must be an instance, else an
	 * exception will be thrown
	 * @return an {@link java.lang.Object} that can be converted to the given
	 * class, or null if the query produced no result
	 * @throws ru.dmerkushov.dbhelper.DbHelperException 1. Any underlying
	 * exception<br>
	 * 2. When the result is not an instance of the class specified, the message
	 * will begin with: "Wrong result type", and after that the specification of
	 * the result type and what was expected
	 */
	public Object performDbQuerySingleResultCheckType (String sql, Object[] sqlParams, String columnLabel, Class clazz) throws DbHelperException {
		loggerWrapper.entering (sql, sqlParams, columnLabel, clazz);

		Object result = performDbQuerySingleResult (sql, sqlParams, columnLabel);

		if (result != null && !clazz.isInstance (result)) {
			throw new DbHelperException ("Wrong result type: result is " + result.getClass ().getCanonicalName () + ", expected " + clazz.getCanonicalName ());
		}

		return result;
	}

	/**
	 * Perform a query and get a single column as a list
	 *
	 * @param sql
	 * @param sqlParams
	 * @param columnLabel
	 * @return List of results, or an empty list (not null) when there were no
	 * results
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public List<Object> performDbQueryList (String sql, Object[] sqlParams, String columnLabel) throws DbHelperException {
		loggerWrapper.entering (sql, sqlParams, columnLabel);

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

		loggerWrapper.exiting (result);
		return result;
	}

	/**
	 * Perform a query and get a single column as a list
	 *
	 * @param sql
	 * @param sqlParams
	 * @param columnIndex the first column is 1, the second is 2, ...
	 * @return List of results, or an empty list (not null) when there were no
	 * results
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public List<Object> performDbQueryList (String sql, Object[] sqlParams, int columnIndex) throws DbHelperException {
		loggerWrapper.entering (sql, sqlParams, columnIndex);

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

		loggerWrapper.exiting (result);
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
		loggerWrapper.entering (sql, params);

		boolean exists;
		try (ResultSet existsRs = performDbQuery (sql, params)) {
			exists = existsRs.next ();
		}

		loggerWrapper.exiting (exists);
		return exists;
	}

	/**
	 * Perform an update to the database
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Query parameters. Supported types are:
	 * {@link String}, {@link Boolean}, {@link Long}, {@link Integer}, {@link Double}, {@link Float}, {@link java.sql.Time}, {@link java.sql.Timestamp}, {@link java.sql.Date}
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 * @throws IllegalArgumentException If one or more of the params is not of
	 * supported class
	 */
	public int performDbUpdate (String sql, Object... sqlParams) throws DbHelperException {
		Objects.requireNonNull (sql, "sql");
		if (sql.equals ("")) {
			throw new IllegalArgumentException ("SQL provided is empty");
		}

		loggerWrapper.entering (sql, sqlParams);

		Integer toReturn = null;
		PreparedStatement ps;
		try {
			ps = prepareStatement (sql, sqlParams);
		} catch (DbHelperException ex) {
			throw new DbHelperException ("Received a DbHelperException when trying to prepare statement for SQL: \"" + sql + "\".", ex);
		}

		try {
			toReturn = ps.executeUpdate ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to execute query for SQL: \"" + sql + "\".", ex);
		}

		loggerWrapper.exiting (toReturn);

		return toReturn;
	}

	/**
	 * Get the underlying DB connection
	 *
	 * @return
	 */
	public Connection getDbConnection () {
		return dbConnection;
	}

	/**
	 * Create a DB connection, NOT forcing the re-creation if it is considered
	 * OK
	 *
	 * @throws DbHelperException
	 */
	public void openDbConnection () throws DbHelperException {
		loggerWrapper.entering ();

		openDbConnection (false);

		loggerWrapper.exiting ();
	}

	/**
	 * Create a DB connection
	 *
	 * @param forceRecreation Whether to force re-creation if it is considered
	 * OK
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void openDbConnection (boolean forceRecreation) throws DbHelperException {
		loggerWrapper.entering (forceRecreation);

		if (driverName == null) {
			throw new DbHelperException ("driverName supplied is null");
		}
		if (driverName.equals ("")) {
			throw new DbHelperException ("driverName supplied is empty (not null)");
		}

		/**
		 * A flag to indicate whether we really need to open a new connection
		 */
		boolean needOpenDbConnection = forceRecreation;

		if (dbConnection == null) {
			needOpenDbConnection = true;
		} else {
			try {
				needOpenDbConnection = needOpenDbConnection || (dbConnection.isClosed () && !dbConnection.isValid (0));
			} catch (SQLException ex) {
				// isValid is not supported by some ancient JDBC drivers, so we shall perform some additional checks
				try {
					needOpenDbConnection = needOpenDbConnection || dbConnection.isClosed ();
					if (!needOpenDbConnection) {
						// Trying to set autocommit mode several times.
						// It's not a universal measure of connection validity
						// (the driver can store the value on the client side, for example),
						// but just something we can do
						boolean autoCommit = dbConnection.getAutoCommit ();
						dbConnection.setAutoCommit (false);
						dbConnection.setAutoCommit (true);
						dbConnection.setAutoCommit (autoCommit);
						needOpenDbConnection = needOpenDbConnection || false;
					}
				} catch (SQLException ex1) {
					needOpenDbConnection = true;
				}
			}
			if (needOpenDbConnection) {
				try {
					dbConnection.close ();	// If the connection is closed, nothing will happen; if opened, should close it first before re-opening
				} catch (SQLException ex) {
					throw new DbHelperException (ex);
				}
			}
		}

		if (needOpenDbConnection) {
			loggerWrapper.info ("Need to open a connection");

			try {
				Class.forName (driverName);
			} catch (ClassNotFoundException ex) {
				throw new DbHelperException ("Received a ClassNotFoundException when trying to initialize a class for the database driver: " + driverName, ex);
			}

			loggerWrapper.finer ("Found class for driver name: " + driverName);

			if (username == null) {
				try {
					dbConnection = DriverManager.getConnection (connectionUrl);
				} catch (SQLException ex) {
					throw new DbHelperException ("Received a SQLException when trying to get a connection (by connection URL).", ex);
				}
			} else {
				try {
					dbConnection = DriverManager.getConnection (connectionUrl, username, password);
				} catch (SQLException ex) {
					throw new DbHelperException ("Received a SQLException when trying to get a connection (by connection URL, username and password).", ex);
				}
			}

			loggerWrapper.finer ("Got a connection from the driver");

			try {
				dbConnection.setAutoCommit (true);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to set autocommit on", ex);
			}

			loggerWrapper.info ("Autocommit is set to true");

			if (driverName.equals ("com.informix.jdbc.IfxDriver")) {
				loggerWrapper.info ("Informix driver. Need to set DIRTY READ mode");
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
				loggerWrapper.info ("DIRTY READ mode set");
			}
		} else {
			loggerWrapper.info ("Do not need to open a connection");
		}

		loggerWrapper.exiting ();
	}

	/**
	 * Set the auto-commit state of the connection
	 *
	 * @param autoCommit
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void setAutoCommit (boolean autoCommit) throws DbHelperException {
		loggerWrapper.entering (autoCommit);

		openDbConnection ();

		try {
			dbConnection.setAutoCommit (autoCommit);
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to set autocommit to " + autoCommit + ".", ex);
		}

		loggerWrapper.exiting ();
	}

	/**
	 * Get the auto-commit state of the connection
	 *
	 * @return
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public boolean getAutoCommit () throws DbHelperException {
		loggerWrapper.entering ();

		openDbConnection ();

		boolean autoCommit;
		try {
			autoCommit = dbConnection.getAutoCommit ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to get autocommit for the connection.", ex);
		}

		loggerWrapper.exiting (autoCommit);
		return autoCommit;
	}

	/**
	 * Commit the current transaction
	 *
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void commit () throws DbHelperException {
		loggerWrapper.entering ();

		openDbConnection ();

		try {
			dbConnection.commit ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to commit transaction.", ex);
		}

		loggerWrapper.exiting ();
	}

	/**
	 * Rollback the current transaction
	 *
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public void rollback () throws DbHelperException {
		loggerWrapper.entering ();

		openDbConnection ();

		try {
			dbConnection.rollback ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to rollback transaction.", ex);
		}

		loggerWrapper.exiting ();
	}

	/**
	 * Release the connection
	 *
	 * @throws ru.dmerkushov.dbhelper.DbHelperException
	 */
	public synchronized void releaseConnection () throws DbHelperException {
		loggerWrapper.entering ();

		if (dbConnection != null) {
			try {
				dbConnection.close ();
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to close the connection.", ex);
			}
			dbConnection = null;
		}

		loggerWrapper.exiting ();
	}

	@Override
	protected void finalize () throws Throwable {
		super.finalize ();
		this.releaseConnection ();
	}

	private PreparedStatement prepareStatement (String sql, Object[] sqlParams) throws DbHelperException {
		openDbConnection ();

		if (dbConnection == null) {
			throw new DbHelperException ("Could not open a database connection");
		}

		PreparedStatement ps;
		try {
			ps = dbConnection.prepareStatement (sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		if (sqlParams != null) {
			for (int paramIndex = 0; paramIndex < sqlParams.length; paramIndex++) {
				Object param = sqlParams[paramIndex];
				if (param == null) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is null");
					try {
						ps.setObject (paramIndex + 1, null);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a null parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.lang.String) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a String: " + (String) param);
					try {
						ps.setString (paramIndex + 1, (String) param);					// i+1, because the first parameter for PreparedStatement.setX() functions is #1
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a String parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.lang.Boolean) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Boolean: " + String.valueOf (param));

					try {
						if (ps.getConnection ().getMetaData ().getDriverName ().contains ("Informix")) {	// Informix JDBC driver has no direct support for setBoolean()
							loggerWrapper.info ("Database type is Informix, must use strings \"T\"/\"F\" for boolean");
							ps.setString (paramIndex + 1, (Boolean) param ? "t" : "f");
						} else {
							ps.setBoolean (paramIndex + 1, (Boolean) param);
						}
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a Boolean parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.lang.Long) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Long: " + String.valueOf (param));

					try {
						ps.setLong (paramIndex + 1, (Long) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a Long parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.lang.Integer) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is an Integer: " + String.valueOf (param));

					try {
						ps.setInt (paramIndex + 1, (Integer) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set an Integer parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.lang.Double) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Double: " + String.valueOf (param));

					try {
						ps.setDouble (paramIndex + 1, (Double) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a Double parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.lang.Float) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a Float: " + String.valueOf (param));

					try {
						ps.setFloat (paramIndex + 1, (Float) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a Float parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.sql.Timestamp) {

					SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Timestamp: " + sdf.format ((java.sql.Timestamp) param));

					try {
						ps.setTimestamp (paramIndex + 1, (java.sql.Timestamp) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.sql.Time) {

					SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Time: " + sdf.format ((java.sql.Time) param));

					try {
						ps.setTime (paramIndex + 1, (java.sql.Time) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof java.sql.Date) {

					SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a java.sql.Date: " + sdf.format ((java.sql.Date) param));

					try {
						ps.setDate (paramIndex + 1, (java.sql.Date) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else if (param instanceof byte[]) {

					loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (paramIndex).trim () + " is a byte array");

					try {
						ps.setBytes (paramIndex + 1, (byte[]) param);
					} catch (SQLException ex) {
						throw new DbHelperException ("Received a SQLException when trying to set a byte array parameter #" + String.valueOf (paramIndex).trim () + " for SQL: \"" + sql + "\".", ex);
					}

				} else {
					IllegalArgumentException iae = new IllegalArgumentException ("Illegal class of parameter #" + String.valueOf (paramIndex).trim () + ": " + (param != null ? param.getClass ().getName () : "null") + ".\n SQL is \"" + sql + "\".\n Supported classes are: String, Boolean, Long, Integer, Double, Float, and java.sql.Date");
					throw iae;
				}
			}
		}

		return ps;
	}

}
