/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Modified BSD License
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at:
//
// http://opensource.org/licenses/BSD-3-Clause
*/
package sqlline;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;

/**
 * Abstract base class representing a set of rows to be displayed.
 */
abstract class Rows implements Iterator<Rows.Row> {
  protected final SqlLine sqlLine;
  final ResultSetMetaData rsMeta;
  final Boolean[] primaryKeys;
  final NumberFormat numberFormat;

  Rows(SqlLine sqlLine, ResultSet rs) throws SQLException {
    this.sqlLine = sqlLine;
    rsMeta = rs.getMetaData();
    int count = rsMeta.getColumnCount();
    primaryKeys = new Boolean[count];
    if (sqlLine.getOpts().getNumberFormat().equals("default")) {
      numberFormat = null;
    } else {
      numberFormat = new DecimalFormat(sqlLine.getOpts().getNumberFormat());
    }
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Update all of the rows to have the same size, set to the
   * maximum length of each column in the Rows.
   */
  abstract void normalizeWidths();

  /**
   * Return whether the specified column (0-based index) is
   * a primary key. Since this method depends on whether
   * the JDBC driver property implements
   * {@link java.sql.ResultSetMetaData#getTableName} (many do not), it
   * is not reliable for all databases.
   */
  boolean isPrimaryKey(int col) {
    if (primaryKeys[col] != null) {
      return primaryKeys[col];
    }

    try {
      // this doesn't always work, since some JDBC drivers (e.g.,
      // Oracle's) return a blank string from getTableName.
      String table = rsMeta.getTableName(col + 1);
      String column = rsMeta.getColumnName(col + 1);

      if (table == null
          || table.length() == 0
          || column == null
          || column.length() == 0) {
        return primaryKeys[col] = false;
      }

      ResultSet pks =
          sqlLine.getDatabaseConnection().meta.getPrimaryKeys(
              sqlLine.getDatabaseConnection().meta.getConnection().getCatalog(),
              null,
              table);

      try {
        while (pks.next()) {
          if (column.equalsIgnoreCase(
              pks.getString("COLUMN_NAME"))) {
            return primaryKeys[col] = true;
          }
        }
      } finally {
        pks.close();
      }

      return primaryKeys[col] = false;
    } catch (SQLException sqle) {
      return primaryKeys[col] = false;
    }
  }

  /** Row from a result set. */
  class Row {
    final String[] values;
    final boolean isMeta;
    protected boolean deleted;
    protected boolean inserted;
    protected boolean updated;
    protected int[] sizes;

    Row(int size) throws SQLException {
      isMeta = true;
      values = new String[size];
      sizes = new int[size];
      for (int i = 0; i < size; i++) {
        values[i] = rsMeta.getColumnLabel(i + 1);
        sizes[i] = values[i] == null ? 1 : values[i].length();
      }

      deleted = false;
      updated = false;
      inserted = false;
    }

    Row(int size, ResultSet rs) throws SQLException {
      isMeta = false;
      values = new String[size];
      sizes = new int[size];

      try {
        deleted = rs.rowDeleted();
      } catch (Throwable t) {
        // ignore
      }
      try {
        updated = rs.rowUpdated();
      } catch (Throwable t) {
        // ignore
      }
      try {
        inserted = rs.rowInserted();
      } catch (Throwable t) {
        // ignore
      }

      for (int i = 0; i < size; i++) {
        final Object o;
        switch (rs.getMetaData().getColumnType(i + 1)) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
        case Types.DECIMAL:
        case Types.NUMERIC:
          o = rs.getObject(i + 1);
          if (o == null) {
            values[i] = "null";
          } else if (numberFormat != null) {
            values[i] = numberFormat.format(o);
          } else {
            values[i] = o.toString();
          }
          break;
        case Types.CLOB:
          if (!sqlLine.getOpts().getShouldReadBlobFields()) {
            break;
          }
          Clob clob = rs.getClob(i + 1);
          if (clob == null) {
            values[i] = "null";
          } else {
            values[i] = readClob(clob);
          }
          break;
        case Types.BLOB:
          if (!sqlLine.getOpts().getShouldReadBlobFields()) {
            break;
          }
          Blob blob = rs.getBlob(i + 1);
          if (blob == null) {
            values[i] = "null";
          } else {
            values[i] = readBlob(blob);
          }
          break;
        case Types.BIT:
        case Types.REF:
        case Types.JAVA_OBJECT:
        case Types.STRUCT:
        case Types.ROWID:
        case Types.NCLOB:
        case Types.SQLXML:
          o = rs.getObject(i + 1);
          if (o == null) {
            values[i] = "null";
          } else {
            values[i] = o.toString();
          }
          break;
        default:
          values[i] = rs.getString(i + 1);
          break;
        }
        sizes[i] = values[i] == null ? 1 : values[i].length();
      }
    }

    private String readClob(Clob clob) throws SQLException {
      int blobStartOffset = sqlLine.getOpts().getBlobStartOffset();
      int blobReadLength = sqlLine.getOpts().getBlobReadLength();
      return clob.getSubString(blobStartOffset, blobReadLength);
    }

    private String readBlob(Blob blob) throws SQLException {
      int blobStartOffset = sqlLine.getOpts().getBlobStartOffset();
      int blobReadLength = sqlLine.getOpts().getBlobReadLength();
      return new String(blob.getBytes(blobStartOffset, blobReadLength));
    }

  }
}

// End Rows.java
