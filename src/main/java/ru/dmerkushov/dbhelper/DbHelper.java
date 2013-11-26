/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.dmerkushov.dbhelper;

import java.sql.*;
import java.text.SimpleDateFormat;
import ru.dmerkushov.loghelper.LoggerWrapper;

/**
 *
 * @author Dmitriy Merkushov
 */
public class DbHelper {

	Connection dbConnection = null;
	String driverName = null;
	String connectionUrl = null;
	LoggerWrapper loggerWrapper = LoggerWrapper.getLoggerWrapper ("DbHelper");

	/**
	 *
	 * @param driverName JDBC driver class name (i.e.,
	 * "com.informix.jdbc.IfxDriver")
	 * @param connectionUrl
	 */
	public DbHelper (String driverName, String connectionUrl) {
		loggerWrapper.configureByDefaultDailyRolling ("log/DbHelper_%d_%u.log");

		Object[] params = {driverName, connectionUrl};
		loggerWrapper.entering (params);

		this.driverName = driverName;
		this.connectionUrl = connectionUrl;

		loggerWrapper.exiting ();
	}

	/**
	 * Performs a query to a DB, using SQL as a query
	 *
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultSet performDbQuery (String sql) throws DbHelperException {
		Object[] methodParams = {sql};
		loggerWrapper.entering (methodParams);

		ResultSet resultSet = performDbQuery (sql, null);

		loggerWrapper.exiting (resultSet);

		return resultSet;
	}
	
	/**
	 * Performs a query to the database
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String, Boolean,
	 * Long, Integer, Double, Float, and java.sql.Date
	 * @return
	 * @throws DbHelperException
	 * @throws IllegalArgumentException If one or more of the params is not of
	 * supported class
	 * @see String
	 * @see Boolean
	 * @see Long
	 * @see Integer
	 * @see Double
	 * @see Float
	 * @see java.sql.Date
	 */
	public ResultSet performDbQuery (String sql, Object[] sqlParams) throws DbHelperException {
		Object[] methodParams = {sql, sqlParams};
		loggerWrapper.entering (methodParams);

		ResultSet toReturn = null;
		PreparedStatement ps = null;

		openDbConnection ();

		if (dbConnection == null) {
			throw new DbHelperException ("Database connection is null");
		}
		if (sql == null) {
			throw new DbHelperException ("SQL provided is null");
		}

		if (dbConnection != null) {
			loggerWrapper.info ("Preparing a statement for SQL: \"" + sql + "\"");
			try {
				ps = dbConnection.prepareStatement (sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to prepare statement for SQL: \"" + sql + "\".", ex);
			}

			loggerWrapper.info ("PreparedStatement for SQL: \"" + sql + "\" prepared");

			try {
				if (sqlParams != null) {

					loggerWrapper.info ("Running through parameters for SQL: \"" + sql + "\"");

					for (int i = 0; i < sqlParams.length; i++) {
						Object param = sqlParams[i];

						if (param instanceof java.lang.String) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a String: " + (String) param);
							try {
								ps.setString (i + 1, (String) param);					// i+1, because the first parameter for PreparedStatement.setX() functions is #1
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a String parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Boolean) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Boolean: " + String.valueOf (param));

							try {
								if (ps.getConnection ().getMetaData ().getDriverName ().contains ("Informix")) {	// Informix JDBC driver has no direct support for setBoolean()
									loggerWrapper.info ("Database type is Informix, must use strings \"T\"/\"F\" for boolean");
									ps.setString (i + 1, (Boolean) param ? "t" : "f");
								} else {
									ps.setBoolean (i + 1, (Boolean) param);
								}
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Boolean parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Long) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Long: " + String.valueOf (param));

							try {
								ps.setLong (i + 1, (Long) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Long parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Integer) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is an Integer: " + String.valueOf (param));

							try {
								ps.setInt (i + 1, (Integer) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set an Integer parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Double) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Double: " + String.valueOf (param));

							try {
								ps.setDouble (i + 1, (Double) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Double parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Float) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Float: " + String.valueOf (param));

							try {
								ps.setFloat (i + 1, (Float) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Float parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Date) {


							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a java.sql.Date: " + sdf.format ((java.sql.Date) param));

							try {
								ps.setDate (i + 1, (java.sql.Date) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else {
							IllegalArgumentException iae = new IllegalArgumentException ("Illegal class of parameter #" + String.valueOf (i).trim () + ": " + param.getClass ().getName () + ".\n SQL is \"" + sql + "\".\n Supported classes are: String, Boolean, Long, Integer, Double, Float, and java.sql.Date");
							throw iae;
						}
					}
				}

				loggerWrapper.info ("Executing query for SQL: \"" + sql + "\"");

				try {
					toReturn = ps.executeQuery ();
				} catch (SQLException ex) {
					throw new DbHelperException ("Received a SQLException when trying to execute query for SQL: \"" + sql + "\".", ex);
				}

			} finally {
			}

		}

		loggerWrapper.exiting (toReturn);

		return toReturn;
	}

    /**
     * <p>Gets the value of the designated column in the first row
     * of the <code>ResultSet</code> object denoted by the query and parameters as
     * an <code>Object</code> in the Java programming language.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
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
     * data of SQL user-defined types.  When a column contains
     * a structured or distinct value, the behavior of this method is as
     * if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String, Boolean,
	 * Long, Integer, Double, Float, and java.sql.Date
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>java.lang.Object</code> holding the column value
     */
	public Object performDbQuerySingleResult (String sql, Object[] sqlParams, String columnLabel) throws DbHelperException {
		Object[] methodParams = {sql, sqlParams, columnLabel};
		loggerWrapper.entering (methodParams);
		
		ResultSet rs = this.performDbQuery (sql, sqlParams);
		
		boolean success;
		try {
			success = rs.first ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}
		
		if (!success) {
			throw new DbHelperException ("No rows in result set");
		}
		
		Object result;
		try {
			result = rs.getObject (columnLabel);
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}
		
		loggerWrapper.exiting (result);
		return result;
	}

    /**
     * <p>Gets the value of the designated column in the first row
     * of the <code>ResultSet</code> object denoted by the query and parameters as
     * an <code>Object</code> in the Java programming language.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
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
     * data of SQL user-defined types.  When a column contains
     * a structured or distinct value, the behavior of this method is as
     * if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String, Boolean,
	 * Long, Integer, Double, Float, and java.sql.Date
     * @param columnIndex the first column is 1, the second is 2, ...
    * @return a <code>java.lang.Object</code> holding the column value
     */
	public Object performDbQuerySingleResult (String sql, Object[] sqlParams, int columnIndex) throws DbHelperException {
		Object[] methodParams = {sql, sqlParams, columnIndex};
		loggerWrapper.entering (methodParams);
		
		ResultSet rs = this.performDbQuery (sql, sqlParams);
		
		boolean success;
		try {
			success = rs.first ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}
		
		if (!success) {
			throw new DbHelperException ("No rows in result set");
		}
		
		Object result;
		try {
			result = rs.getObject (columnIndex);
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}
		
		loggerWrapper.exiting (result);
		return result;
	}

	/**
	 * Performs an update to the database
	 *
	 * @param sql SQL code, where question marks (?) are placeholders for
	 * parameters
	 * @param sqlParams Array of parameters. Supported types are: String, Boolean,
	 * Long, Integer, Double, Float, and java.sql.Date
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 * statements or (2) 0 for SQL statements that return nothing
	 * @throws DbHelperException
	 * @throws IllegalArgumentException If one or more of the params is not of
	 * supported class
	 * @see String
	 * @see Boolean
	 * @see Long
	 * @see Integer
	 * @see Double
	 * @see Float
	 * @see java.sql.Date
	 */
	public int performDbUpdate (String sql, Object[] sqlParams) throws DbHelperException {
		Object[] methodParams = {sql, sqlParams};
		loggerWrapper.entering (methodParams);

		Integer toReturn = null;
		PreparedStatement ps = null;

		openDbConnection ();

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
			loggerWrapper.info ("Preparing a statement for SQL: \"" + sql + "\"");
			try {
				ps = dbConnection.prepareStatement (sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to prepare statement for SQL: \"" + sql + "\".", ex);
			}

			loggerWrapper.info ("PreparedStatement for SQL: \"" + sql + "\" prepared");

			try {
				if (sqlParams != null) {

					loggerWrapper.info ("Running through parameters for SQL: \"" + sql + "\"");

					for (int i = 0; i < sqlParams.length; i++) {
						Object param = sqlParams[i];

						if (param instanceof java.lang.String) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a String: " + (String) param);
							try {
								ps.setString (i + 1, (String) param);					// i+1, because the first parameter for PreparedStatement.setX() functions is #1
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a String parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Boolean) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Boolean: " + String.valueOf (param));

							try {
								if (ps.getConnection ().getMetaData ().getDriverName ().contains ("Informix")) {	// Informix JDBC driver has no direct support for setBoolean()
									loggerWrapper.info ("Database type is Informix, must use strings \"T\"/\"F\" for boolean");
									ps.setString (i + 1, (Boolean) param ? "t" : "f");
								} else {
									ps.setBoolean (i + 1, (Boolean) param);
								}
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Boolean parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Long) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Long: " + String.valueOf (param));

							try {
								ps.setLong (i + 1, (Long) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Long parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Integer) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is an Integer: " + String.valueOf (param));

							try {
								ps.setInt (i + 1, (Integer) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set an Integer parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Double) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Double: " + String.valueOf (param));

							try {
								ps.setDouble (i + 1, (Double) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Double parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.lang.Float) {

							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a Float: " + String.valueOf (param));

							try {
								ps.setFloat (i + 1, (Float) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a Float parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else if (param instanceof java.sql.Date) {


							SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
							loggerWrapper.info ("Parameter for SQL: \"" + sql + "\" #" + String.valueOf (i).trim () + " is a java.sql.Date: " + sdf.format ((java.sql.Date) param));

							try {
								ps.setDate (i + 1, (java.sql.Date) param);
							} catch (SQLException ex) {
								throw new DbHelperException ("Received a SQLException when trying to set a java.sql.Date parameter #" + String.valueOf (i).trim () + " for SQL: \"" + sql + "\".", ex);
							}

						} else {
							IllegalArgumentException iae = new IllegalArgumentException ("Illegal class of parameter #" + String.valueOf (i).trim () + ": " + param.getClass ().getName () + ".\n SQL is \"" + sql + "\".\n Supported classes are: String, Boolean, Long, Integer, Double, Float, and java.sql.Date");
							throw iae;
						}
					}
				}

				loggerWrapper.info ("Executing update for SQL: \"" + sql + "\"");

				try {
					toReturn = ps.executeUpdate ();
				} catch (SQLException ex) {
					throw new DbHelperException ("Received a SQLException when trying to execute query for SQL: \"" + sql + "\".", ex);
				}

			} finally {
			}

		}

		loggerWrapper.exiting (toReturn);

		return toReturn;
	}

	/**
	 * Create a DB connection
	 * data
	 *
	 * @throws DbHelperException
	 */
	public void openDbConnection () throws DbHelperException {
		loggerWrapper.entering ();

		if (driverName == null) {
			throw new DbHelperException ("driverName supplied is null");
		}
		if (driverName.equals ("")) {
			throw new DbHelperException ("driverName supplied is empty");
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
			}
			 */
		}

		if (doOpenDbConnection) {
			loggerWrapper.info ("Need to open a connection");

			try {
				Class.forName (driverName);
			} catch (ClassNotFoundException ex) {
				throw new DbHelperException ("Received a ClassNotFoundException when trying to initialize a class for the database driver: " + driverName, ex);
			}

			loggerWrapper.info ("Found class for driver name: " + driverName);

			try {
				dbConnection = DriverManager.getConnection (connectionUrl);
			} catch (SQLException ex) {
				throw new DbHelperException ("Received a SQLException when trying to get a connection.", ex);
			}

			loggerWrapper.info ("Got a connection from the driver");

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
	
	public void setAutoCommit (boolean autoCommit) throws DbHelperException {
		Object[] methodParams = {autoCommit};
		loggerWrapper.entering (methodParams);
		try {
			dbConnection.setAutoCommit (autoCommit);
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to set autocommit to " + autoCommit + ".", ex);
		}
		
		loggerWrapper.exiting ();
	}
	
	public boolean getAutoCommit () throws DbHelperException {
		loggerWrapper.entering ();
		
		boolean autoCommit;
		try {
			autoCommit = dbConnection.getAutoCommit ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to get autocommit for the connection.", ex);
		}
		
		loggerWrapper.exiting (autoCommit);
		return autoCommit;
	}
	
	public void commit () throws DbHelperException {
		loggerWrapper.entering ();

		try {
			dbConnection.commit ();
		} catch (SQLException ex) {
			throw new DbHelperException ("Received a SQLException when trying to commit transaction.", ex);
		}
		
		loggerWrapper.exiting ();
	}
	
	public void rollback () throws DbHelperException {
		loggerWrapper.entering ();
		
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
	 * @throws DbHelperException
	 */
	public void releaseConnection () throws DbHelperException {
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
		loggerWrapper.entering ();
		this.releaseConnection ();
		loggerWrapper.exiting ();
	}
}
