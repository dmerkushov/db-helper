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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import org.apache.xerces.dom.DocumentImpl;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 * Utility methods to use with ResultSets
 * @author Dmitriy Merkushov
 */
public class ResultSetUtils {


	/**
	 * Put the contents of a ResultSet to a DOM document. Does not save the position in the ResultSet.<br/>
	 * <br/>
	 * The DOM output is a one like the following XML represents:<br/>
	 * <pre>
	 * &lt;?xml version="1.0" encoding="UTF-8"?>
	 * &lt;recordset>
	 *     &lt;record>
	 *         &lt;column name="field_name_1" type="BIGINT UNSIGNED">1&lt;/column>
	 *         &lt;column name="field_name_2" type="CHAR">Olala&lt;/column>
	 *         &lt;column name="field_name_3" type="TIMESTAMP">2013-12-19 21:07:02.0&lt;/column>
	 *     &lt;/record>
	 * &lt;/recordset>
	 * </pre>
	 *
	 * @param rs The ResultSet. The method will try to go it through, from the beginning to the end, but if the JDBC driver doesn't support {@link java.sql.ResultSet#beforeFirst() } method, or a SQLException happens, will begin at the next row after the current one
	 * @return
	 * @throws DbHelperException
	 */
	public static Document resultSetToDomDocument (ResultSet rs) throws DbHelperException, SQLException {
		DbHelper.getLoggerWrapper ().entering (rs);
		
		DocumentImpl document = new DocumentImpl ();

		ResultSetMetaData rsMeta;
		try {
			rsMeta = rs.getMetaData ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		int columnCount;
		try {
			columnCount = rsMeta.getColumnCount ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		try {
			rs.beforeFirst ();
		} catch (SQLException ex) {
			// ignore
		}

		Node rootNode = document.createElement ("recordset");
		document.appendChild (rootNode);

		boolean hasNext;
		try {
			hasNext = rs.next ();
		} catch (SQLException ex) {
			throw new DbHelperException (ex);
		}

		SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

		while (hasNext) {
			Node recordNode = document.createElement ("record");
			rootNode.appendChild (recordNode);

			for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
				Node columnNode = document.createElement ("column");
				recordNode.appendChild (columnNode);

				Attr columnName = document.createAttribute ("name");
				try {
					columnName.setValue (rsMeta.getColumnName (columnIndex));
				} catch (SQLException ex) {
					throw new DbHelperException ("Column " + columnIndex, ex);
				}
				columnNode.getAttributes ().setNamedItem (columnName);

				Attr columnType = document.createAttribute ("type");
				try {
					columnType.setValue (rsMeta.getColumnTypeName (columnIndex));
				} catch (SQLException ex) {
					throw new DbHelperException ("Column " + columnIndex, ex);
				}
				columnNode.getAttributes ().setNamedItem (columnType);
				Object columnValue;
				String columnValueString;

				switch (rsMeta.getColumnType (columnIndex)) {
					case java.sql.Types.CHAR:
					case java.sql.Types.VARCHAR:
					case java.sql.Types.LONGVARCHAR:
						columnValueString = rs.getString (columnIndex);
						break;
					case java.sql.Types.NCHAR:
					case java.sql.Types.NVARCHAR:
					case java.sql.Types.LONGNVARCHAR:
						columnValueString = rs.getNString (columnIndex);
						break;
					case java.sql.Types.BIGINT:
					case java.sql.Types.INTEGER:
					case java.sql.Types.SMALLINT:
					case java.sql.Types.TINYINT:
						columnValueString = String.valueOf (rs.getLong (columnIndex));
						break;
					case java.sql.Types.DECIMAL:
					case java.sql.Types.NUMERIC:
						columnValue = rs.getBigDecimal (columnIndex);
						columnValueString = (columnValue != null ? ((BigDecimal) columnValue).toString () : null);
						break;
					case java.sql.Types.DOUBLE:
						columnValueString = String.valueOf (rs.getDouble (columnIndex));
						break;
					case java.sql.Types.FLOAT:
					case java.sql.Types.REAL:
						columnValueString = String.valueOf (rs.getFloat (columnIndex));
						break;
					case java.sql.Types.BOOLEAN:
						columnValueString = String.valueOf (rs.getBoolean (columnIndex));
						break;
					case java.sql.Types.DATE:
						columnValue = rs.getDate (columnIndex);
						columnValueString = (columnValue != null ? sdf.format ((java.sql.Date) columnValue) : null);
						break;
					case java.sql.Types.TIME:
						columnValue = rs.getTime (columnIndex);
						columnValueString = (columnValue != null ? sdf.format ((java.sql.Time) columnValue) : null);
						break;
					case java.sql.Types.TIMESTAMP:
						columnValue = rs.getTimestamp (columnIndex);
						columnValueString = (columnValue != null ? sdf.format ((java.sql.Timestamp) columnValue) : null);
						break;
					default:
						DbHelper.getLoggerWrapper ().warning ("Unknown type of column " + columnIndex + ": " + rsMeta.getColumnTypeName (columnIndex) + ". Will set the value to null in the DOM document.");
						columnValueString = null;
						break;
				}
				if (columnValueString != null) {
					columnNode.setTextContent (columnValueString);
				}
			}

			try {
				hasNext = rs.next ();
			} catch (SQLException ex) {
				throw new DbHelperException (ex);
			}
		}

		DbHelper.getLoggerWrapper ().exiting (document);
		return document;
	}
}
