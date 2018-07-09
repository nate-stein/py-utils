"""
Script to update database rows where a player's name is not in the desired form.
"""

import dfs.utils.main as dfm
import dfs.db.qual_ctrl as dqc
import dfs.utils.db as udb

name_conversion = dqc.load_same_name_pairs()


def get_new_name (name):
    if name in name_conversion:
        return name_conversion[name]
    return None


def create_sql_name_update (row, idx_col, name_field, table):
    """Assumes database index column (`idx_col`) is numeric."""
    idx = row[idx_col]
    new_name = udb.clean_str_for_sql(row['new_name'])
    s = "UPDATE {} SET {} = '{}' WHERE {} = {}".format(table, name_field,
                                                       new_name, idx_col, idx)
    return s


to_update = [('SELECT * FROM injuries', 'injuries', 'player', 'id'),
             ('SELECT * FROM news', 'news', 'player', 'id'),
             ('SELECT * FROM playerstats WHERE season IN (2017, 2018)',
              'playerstats', 'player', 'id')]

all_queries = []
db = dfm.create_nba_dbwrapper()

# Build all queries.
for (sql, table, name_col, idx_col) in to_update:
    df = db.query_df(sql)
    df['new_name'] = df[name_col].apply(get_new_name)
    df = df[df['new_name'].notnull()]
    df['update_sql'] = df.apply(create_sql_name_update, axis=1,
                                args=(idx_col, name_col, table))
    queries = df['update_sql'].tolist()
    all_queries = all_queries + queries

# Execute all queries.
db_update = udb.DBUpdate(db)
log_path = dfm.Folders.DFExperiments + 'update_names'
db_update.update(all_queries, log_path)
