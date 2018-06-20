"""
pyodbc engine wrapper for simple methods permitting edit access to database.
"""
import enum
import pyodbc
from typing import Any

import dfs.utils.data_utils as udu
import dfs.utils.io as uio
import numpy as np
from pandas import DataFrame
import pandas as pd


class DBError(Exception):
    """Encapsulates errors encountered in module.

    Attributes:
        method (str): Name of method in which error was encountered.
    """

    def __init__ (self, method, msg=None, original_exception=None):
        if msg is None:
            msg = 'Error encountered in {}.'.format(method)
        Exception.__init__(self, msg)
        self.method = method
        self.original_exception = original_exception


class DBWrapper(object):
    """Wraps pyodbc functionality to access database and perform edits.
    
    Notes:
        Maintains error log of sql query attempts for post-execution review.

    Attributes:
        conn_str (str): Connection string.
        conn (pyodbc.Connection): Database connection initialized with conn_str.
        cursor (pyodbc.Cursor): Database connection cursor.
        db_name (str, optional): Name for database.
        log (DataFrame): Execution log for every sql query to track whether
        it was successful or not.
    """

    def __init__ (self, conn_str, db_name=None):
        """Inits Connection object using pyodbc.connect() and creates Cursor
        object with that Connection.
        """
        try:
            self.conn_str = conn_str
            self.conn = pyodbc.connect(conn_str, autocommit=False)
            self.cursor = self.conn.cursor()
            self.db_name = db_name
            self.log = DataFrame(columns=['success', 'sql_query', 'err_info'])
        except Exception as e:
            self.close_resources()
            raise DBError('__init__', 'Failed opening connection to database.',
                          e)

    def execute (self, sql, raise_err=False, log=True):
        """Executes sql query.

        Args:
            sql (str): Query to execute.
            raise_err (bool): If True, then DBError is raised on a failed SQL
            query and exception is not logged; if False, no error is raised
            on failed SQL query but the exception is logged.
        """
        try:
            self.cursor.execute(sql)
            if log:
                self._update_log(True, sql, '')
        except Exception as e:
            if raise_err:
                info = 'Failed to execute following SQL: ' + sql
                raise DBError(self.db_name, info) from e
            if log:
                self._update_log(False, sql, str(e))

    def query_scalar (self, sql, raise_err=True) -> Any:
        """Executes sql query and returns the resulting scalar. It is assumed
        that the sql query only returns a single scalar.
        """
        self.execute(sql, raise_err, log=False)
        return self.cursor.fetchall()[0][0]

    def get_table (self, table, idx_col=None) -> DataFrame:
        """Executes SQL to retrieve all data from a table."""
        sql = 'SELECT * FROM {}'.format(table)
        return pd.read_sql_query(sql, self.conn, index_col=idx_col)

    def query_df (self, sql, idx_col=None):
        """Executes sql query and returns the results in form of a DataFrame."""
        return pd.read_sql_query(sql, self.conn, index_col=idx_col)

    def _update_log (self, success, sql, err_details=''):
        """Adds new record to execution log.

        Args:
            success (bool): True if the SQL execution was successful.
            sql (str): SQL query.
            err_details (str): Additional information about the error.
        """
        row = {'success':success, 'sql_query':sql, 'err_info':err_details}
        self.log = self.log.append(row, ignore_index=True)

    @property
    def successes (self):
        """Returns number of sql queries executed successfully."""
        return len(self.log[self.log['success']==True].index)

    @property
    def failures (self):
        """Returns number of failed sql queries."""
        return len(self.log[self.log['success']==False].index)

    def commit_changes (self):
        """Commits all SQL statements executed on the connection that created
        this cursor, since the last commit.
        """
        self.conn.commit()

    def close_resources (self):
        """Closes connection and cursor, releasing memory from variables.

        Notes:
            Any uncommitted effects of SQL statements on the database from
                this connection will be rolled back.
            FYI cursors are closed automatically when they are deleted (
                typically when they go out of scope), so calling this is not
                usually necessary.
        """
        try:
            self.cursor.close()
            self.conn.close()
        except:
            pass


class DBUpdate(object):
    """Wrapper to execute sql queries on database using DBWrapper.

    Attributes:
        db (DBWrapper): Used to execute queries. Assumed to already be
        initialized once passed to constructor.
    """

    def __init__ (self, db):
        self.db = db

    def update (self, queries, log_path):
        if len(queries)==0:
            print('List of queries to execute is empty. Aborting.')
            return

        print('Executing {} queries...'.format(len(queries)))
        for sql in queries:
            self.db.execute(sql)

        commit_chgs = True

        if self.db.failures==0:
            print('Full success.')
        else:
            print('{} failed. Saving execution log.'.format(self.db.failures))
            try:
                uio.save_df_to_excel(self.db.log, log_path)
            except Exception as e:
                print('Encountered exception saving execution '
                      'log:\n\n{}\n'.format(e))
            if uio.response_no('Would you like to still commit changes?'):
                commit_chgs = False

        if commit_chgs:
            print('Committing updates...')
            self.db.commit_changes()


#####################################################################
# SQL injection methods.

class DT(enum.Enum):
    """Dictates how _value is formatted for SQL query."""
    NUMBER = 1
    STR = 2
    DATE = 3
    BOOL = 4


class FieldValue(object):
    """Stores column name and value for entry into a SQL query.

    Notes:
        The values should be entered in their native Python type. The
        property getter for the value will handle properly formatting the
        value for SQL queries depending on the given data type (DT).
    """

    def __init__ (self, fld, val, dtype=None, clean_str=False):
        self.field = fld
        self.clean_str = clean_str
        self._value = self._set_value(val)
        self.dtype = dtype

    @property
    def value (self):
        """Returns _value properly formatted for SQL string."""
        if self._value is None:
            return 'NULL'
        if self.dtype==DT.STR:
            if self.clean_str:
                value = clean_str_for_sql(self._value)
            else:
                value = self._value
            return "'{}'".format(value)
        elif self.dtype==DT.DATE:
            return clean_date_for_sql(self._value, access_db=True)
        elif self.dtype in [DT.NUMBER, DT.BOOL, None]:
            return '{}'.format(self._value)
        raise DBError('value', 'Unhandled DType.')

    @value.setter
    def value (self, v):
        self._value = self._set_value(v)

    @staticmethod
    def _set_value (value):
        try:
            if np.isnan(value):
                return None
            else:
                return value
        except TypeError:
            return value


def field_name_restricted (fld):
    """Returns True if field name is one restricted by Microsoft Access."""
    if fld.lower() in ['date', 'name']:
        return True
    return False


def create_insert_query (field_values, table):
    """Creates SQL INSERT query.

    Args:
        field_values (list[FieldValue]): Fields to be included in the INSERT
        query.
        table (str): Name of table in which data is to be inserted.

    Returns:
        str: SQL-ready INSERT statement.
    """
    fields, values = '', ''
    # Create fields sxn_break.
    # First make sure none of the field names would lead to syntax errors.
    for fv in field_values:
        if field_name_restricted(fv.field):
            # Surround field name in brackets ([]).
            fv.field = '[{}]'.format(fv.field)
    for fv in field_values[:-1]:
        fields += '{}, '.format(fv.field)
    fields += field_values[-1].field
    # Create VALUES sxn_break.
    for fv in field_values[:-1]:
        values += '{}, '.format(fv.value)
    values += field_values[-1].value
    # Combine all results.
    sql = 'INSERT INTO {0} ({1}) VALUES ({2})'.format(table, fields, values)
    return sql


def clean_str_for_sql (s):
    """Escape single apostrophes with two apostrophes."""
    return s.replace("'", "''")


def clean_date_for_sql (dt, access_db=False):
    """Formats date into SQL-acceptable string.

    Args:
        access_db (bool): Optional, default False. If True, then the date is
        formatted in way acceptable to Microsoft Access engine driver.

    Returns:
        str: SQL-ready string form of a date.
    """
    s = dt.strftime("%Y-%m-%d %H:%M:%S")
    if access_db:
        s = '#{}#'.format(s)
    return s


def create_sql_in_clause (vals):
    """Returns SQL IN clause encapsulating vals argument.

    Examples:
        create_sql_in_clause([1, 2, 3]) = 'IN (1, 2, 3)'
    """
    if not udu.is_collection(vals):
        return 'IN ({})'.format(vals)
    str_list = [str(x) for x in vals]
    return 'IN ({})'.format(', '.join(str_list))
