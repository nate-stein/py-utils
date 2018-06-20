"""
Update NBA database with new data from BigDataBall.

General steps:
*   INSERT teamstats into database.
*   INSERT playerstats into database.
*   UPDATE playerstats with tsid & starter.
"""
import datetime

import dfs
import dfs.db.qual_ctrl as dqc
import dfs.utils.data_utils as udu
import dfs.utils.db as udb
import dfs.utils.main as dfm
import os.path
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

#####################################################################

log = dfs.ProgramTimer(ud_start=True, ud_end=False)
log._start_ud_pre_txt = ''

SEASON = 2018

team_conversion = dfm.create_team_mapping('short_name', 'nba_code')
name_converter = dqc.NameConverter()


def convert_name (name):
    try:
        return name_converter.clean(name)
    except:
        print('Warning: convert_name() failed to convert {}.'.format(name))
        return name


#####################################################################

class CriticalDBError(Exception):
    """Encapsulates errors encountered in this module that should cause
    entire script to abort.
    """

    def __init__ (self, method, info=None):
        msg = 'Error encountered in data_utils.py method: {}.'.format(method)
        if info is not None:
            msg = msg + ' ' + info
        Exception.__init__(self, msg)

        self.method = method
        self.info = info


def create_gid (game_date, home_team, away_team):
    date_str = game_date.strftime('%y%m%d')
    return '{}_{}_{}'.format(date_str, home_team, away_team)


def calc_fdp (row):
    """Should typically be executed towards the end of a database update
    script, once other fields have been added.
    """
    s = 0.0
    points_per_unit = {'pts':1, 'treb':1.2, 'ast':1.5, 'blk':3, 'stl':3,
                       'tov':-1}
    for fld, pts in points_per_unit.items():
        s += (row[fld]*pts)
    return s


#####################################################################

class NewTStats:
    """Produces sql queries used to update teamstats.

    Attributes:
        df (DataFrame): Stores get_final data once it has been formatted. Only
        initialized with a call to create_queries().
        min_gd (datetime.datetime): Earliest game date we care about from
        bigdataball (exclusive).
        max_gd (datetime.datetime): Latest game date we care about from
        bigdataball (inclusive).
    """

    # Database columns by type.
    COLS_INT = ['ast', 'blk', 'dreb', 'fg2a', 'fg2m', 'fg3a', 'fg3m', 'fta',
                'ftm', 'mins', 'oreb', 'ot', 'ot1pts', 'ot2pts', 'ot3pts',
                'ot4pts', 'pf', 'pts', 'q1pts', 'q2pts', 'q3pts', 'q4pts',
                'season', 'stl', 'tov', 'treb']
    COLS_FLOAT = ['deff', 'moneyline', 'oeff', 'open_pts', 'open_spread',
                  'pace', 'poss']
    COLS_NUMERIC = COLS_INT + COLS_FLOAT
    COLS_OT_PERIODS = ['ot1pts', 'ot2pts', 'ot3pts', 'ot4pts']
    COLS_STR = ['gid', 'opp', 'ref_crew1', 'ref_crew2', 'ref_main', 'restdays',
                'start1', 'start2', 'start3', 'start4', 'start5', 'team']
    COLS_BOOL = ['home', 'playoff']
    COLS_DATE = ['gd']
    # Columns we'll need to "clean" before inserting into sql query.
    COLS_CLEAN_STR = ['ref_crew1', 'ref_crew2', 'ref_main', 'restdays',
                      'start1', 'start2', 'start3', 'start4', 'start5']

    def __init__ (self, min_gd, max_gd):
        self.df = None
        self.min_gd = min_gd
        self.max_gd = max_gd

    @staticmethod
    def _create_gid (row):
        """Add game id (gid) field to tstats row."""
        gd = row['gd']
        is_home = row['home']
        if is_home:
            home_tm = row['team']
            away_tm = row['opp']
        else:
            away_tm = row['team']
            home_tm = row['opp']
        return create_gid(gd, home_tm, away_tm)

    @staticmethod
    def _load_new_raw_tstats ():
        """Loads raw team feed from Dropbox excel file into DF."""

        def get_filename ():
            """Creates expected file name using today's date going days back."""
            for i in range(15):
                td = pd.Timedelta(days=i)
                dt = pd.to_datetime('today') - td
                txt = dt.strftime('%m-%d-%Y')
                path = '{}{}{}.xlsx'.format(dfs.Folders.DropboxTeam,
                                            'season-team-feed-', txt)
                if os.path.isfile(path):
                    return path
            raise FileNotFoundError('Unable to find latest team feed.')

        path = get_filename()
        return pd.read_excel(path)

    @staticmethod
    def _update_ref_flds (df):
        """Adds main ref to every other row and 2nd crew ref."""
        mainref_loc = df.columns.get_loc('ref_main')
        crewref1_loc = df.columns.get_loc('ref_crew1')
        crewref2_loc = df.columns.get_loc('ref_crew2')
        for i in range(0, len(df.index), 2):
            # Update 2nd crew refs for this row.
            df.iat[i, crewref2_loc] = df.iat[i + 1, crewref1_loc]
            # Now set both of next row's crew refs to this row's crew refs.
            df.iat[i + 1, crewref1_loc] = df.iat[i, crewref1_loc]
            df.iat[i + 1, crewref2_loc] = df.iat[i, crewref2_loc]
            # Main ref is empty in next row.
            df.iat[i + 1, mainref_loc] = df.iat[i, mainref_loc]
        return df

    @staticmethod
    def _calc_ot (row):
        return int((row['mins'] - 240)/25)

    @staticmethod
    def _add_opp_fld (df):
        """Adds 'opp' column using location of each row to determine
        opponent.
        """
        team_loc = df.columns.get_loc('team')
        opp_loc = df.columns.get_loc('opp')
        for i in range(0, len(df.index), 2):
            df.iat[i, opp_loc] = df.iat[i + 1, team_loc]
            df.iat[i + 1, opp_loc] = df.iat[i, team_loc]
        return df

    def _get_new_formatted_tstats (self):
        df = self._load_new_raw_tstats()
        df.rename(columns={'DATASET':'playoff', 'DATE':'gd', 'TEAMS':'team',
                           'VENUE':'home', '1Q':'q1pts', '2Q':'q2pts',
                           '3Q':'q3pts',
                           '4Q':'q4pts', 'OT1':'ot1pts', 'OT2':'ot2pts',
                           'OT3':'ot3pts', 'OT4':'ot4pts', 'F':'pts',
                           'MIN':'mins',
                           '3P':'fg3m', '3PA':'fg3a', 'FT':'ftm', 'FTA':'fta',
                           'OR':'oreb', 'DR':'dreb', 'TOT':'treb', 'A':'ast',
                           'PF':'pf', 'ST':'stl', 'TO':'tov', 'BL':'blk',
                           'POSS':'poss', 'PACE':'pace', 'OEFF':'oeff',
                           'DEFF':'deff', 'REST DAYS':'restdays',
                           'STARTING LINEUPS':'start1', 'Unnamed: 37':'start2',
                           'Unnamed: 38':'start3', 'Unnamed: 39':'start4',
                           'Unnamed: 40':'start5', 'MAIN REF':'ref_main',
                           'CREW':'ref_crew1', 'OPENING SPREAD':'open_spread',
                           'OPENING TOTAL':'open_pts', 'MONEYLINE':'moneyline'},
                  inplace=True)

        df['gd'] = pd.to_datetime(df['gd'])
        df = df[(df['gd']>self.min_gd) & (df['gd']<=self.max_gd)]
        # Init columns we will fill eventually or can fill now.
        df['opp'] = None
        df['ref_crew2'] = None
        df['season'] = SEASON
        df['ot'] = df.apply(self._calc_ot, axis=1)
        # Update team names and add opponent column afterwards.
        df['team'] = df['team'].map(team_conversion)
        df = self._add_opp_fld(df)
        # Update other fields.
        df['playoff'] = df['playoff'].apply(lambda x:'Regular Season' not in x)
        df['home'] = df['home'].apply(lambda x:x.lower()=='home')
        # Add fields for 2-point field goals and remove old FG columns.
        df['fg2m'] = df['FG'] - df['fg3m']
        df['fg2a'] = df['FGA'] - df['fg3a']
        # Add other fields.
        df['gid'] = df.apply(self._create_gid, axis=1)
        df = self._update_ref_flds(df)
        # Update starter names.
        for starter_field in ['start1', 'start2', 'start3', 'start4', 'start5']:
            df[starter_field] = df[starter_field].apply(convert_name)

        ##################################################
        # Ensure proper dtypes.
        ##################################################
        # Exclude OT period columns b/c they contain zeroes.
        int_cols = [col for col in self.COLS_INT
                    if col not in self.COLS_OT_PERIODS]
        for col in int_cols:
            try:
                df[col] = df[col].astype(int)
            except:
                print('Failed to cast col to int: ', col)
        for col in self.COLS_FLOAT:
            df[col] = df[col].astype(float)
        return df

    def _create_row_sql (self, row):
        """Creates SQL query needed to insert a new tstats row into database."""
        field_vals = []
        for col in self.COLS_NUMERIC:
            fv = udb.FieldValue(col, row[col], dtype=udb.DT.NUMBER)
            field_vals.append(fv)
        for col in self.COLS_STR:
            fv = udb.FieldValue(col, row[col], dtype=udb.DT.STR)
            field_vals.append(fv)
        for col in self.COLS_BOOL:
            fv = udb.FieldValue(col, row[col], dtype=udb.DT.BOOL)
            field_vals.append(fv)
        for col in self.COLS_DATE:
            fv = udb.FieldValue(col, row[col], dtype=udb.DT.DATE)
            field_vals.append(fv)
        return udb.create_insert_query(field_vals, 'teamstats')

    def create_queries (self):
        """Prepares list of SQL queries to update teamstats with new data."""
        df = self._get_new_formatted_tstats()
        self.df = df
        # Ensure player name strings properly formatted.
        for fld in self.COLS_CLEAN_STR:
            df[fld] = df[fld].apply(udb.clean_str_for_sql)
        # Compile queries.
        df['sql'] = df.apply(self._create_row_sql, axis=1)
        queries = df['sql'].tolist()
        return queries

    def date_range (self):
        """Produces tuple with information on game dates spanned by sql queries.

        Returns:
            num_dates (int): Number of unique game dates in data.
            min_date (datetime): Earliest game date in data.
            max_date (datetime): Latest game date in data.
        """
        dates = self.df['gd'].unique().tolist()
        num_dates = len(dates)
        min_date = min(dates)
        max_date = max(dates)
        return num_dates, min_date, max_date


class NewPStats:
    """Produces sql queries used to update playerstats.

    Attributes:
        df (DataFrame): Stores get_final data once it has been formatted. Only
        initialized with a call to create_queries().
        min_gd (datetime.datetime): Earliest game date we care about from
        bigdataball (exclusive).
        max_gd (datetime.datetime): Latest game date we care about from
        bigdataball (inclusive).
    """

    COLS_INT = ['ast', 'blk', 'dreb', 'fg2a', 'fg2m', 'fg3a', 'fg3m', 'fta',
                'ftm', 'oreb', 'pf', 'pts', 'season', 'stl', 'tov', 'treb']
    COLS_FLOAT = ['mins']
    COLS_NUMERIC = COLS_INT + COLS_FLOAT
    COLS_STR = ['player', 'pos', 'team', 'opp', 'gid']
    COLS_BOOL = ['home', 'playoff']
    COLS_DATE = ['gd']
    COLS_CLEAN_STR = ['player']

    def __init__ (self, min_gd, max_gd):
        self.df = None
        self.min_gd = min_gd
        self.max_gd = max_gd

    @staticmethod
    def _create_gid (grp):
        """Add game id (gid) field to game date/team group."""
        row = grp.iloc[0]
        gd = row['gd']
        is_home = row['home']
        if is_home:
            home_tm = row['team']
            away_tm = row['opp']
        else:
            away_tm = row['team']
            home_tm = row['opp']
        grp['gid'] = create_gid(gd, home_tm, away_tm)
        return grp

    @staticmethod
    def _load_new_raw_pstats ():
        """Loads raw player feed from Dropbox excel file into DF."""

        def get_filename ():
            """Creates expected file name using today's date. Loops backwards
            in time if no matching file is found for today's date.
            """
            for i in range(15):
                timedelta = pd.Timedelta(days=i)
                dt = pd.to_datetime('today') - timedelta
                date_str = dt.strftime('%m-%d-%Y')
                path = '{}{}{}.xlsx'.format(dfs.Folders.DropboxPlayer,
                                            'season-player-feed-', date_str)
                if os.path.isfile(path):
                    return path
            raise FileNotFoundError('Unable to find latest player feed.')

        path = get_filename()
        return pd.read_excel(path)

    def _get_new_formatted_pstats (self):
        df = self._load_new_raw_pstats()
        df.rename(columns={'DATA SET':'playoff', 'DATE':'gd',
                           'PLAYER FULL NAME':'player', 'POSITION':'pos',
                           'OWN TEAM':'team', 'OPP TEAM':'opp',
                           'VENUE (R/H)':'home', 'MIN':'mins', '3P':'fg3m',
                           '3PA':'fg3a', 'FT':'ftm', 'FTA':'fta', 'OR':'oreb',
                           'DR':'dreb', 'TOT':'treb', 'A':'ast', 'PF':'pf',
                           'ST':'stl', 'TO':'tov', 'BL':'blk', 'PTS':'pts'},
                  inplace=True)

        df['gd'] = pd.to_datetime(df['gd'])
        df = df[(df['gd']>self.min_gd) & (df['gd']<=self.max_gd)]
        # Update team names.
        df['team'] = df['team'].map(team_conversion)
        df['opp'] = df['opp'].map(team_conversion)
        # Update other fields.
        df['playoff'] = df['playoff'].apply(lambda x:'Regular Season' not in x)
        df['home'] = df['home'].apply(lambda x:x=='H')
        df['season'] = SEASON
        # Add fields for 2-point field goals and remove old FG columns.
        df['fg2m'] = df['FG'] - df['fg3m']
        df['fg2a'] = df['FGA'] - df['fg3a']
        df.drop(['FG', 'FGA'], inplace=True, axis=1)
        # Add game id.
        df = df.groupby(['gd', 'team']).apply(self._create_gid)
        # Update player names.
        df['player'] = df['player'].apply(convert_name)
        ##################################################
        # Ensure proper dtypes.
        ##################################################
        for col in self.COLS_INT:
            df[col] = df[col].astype(int)
        for col in self.COLS_FLOAT:
            df[col] = df[col].astype(float)
        return df

    def _create_row_sql (self, row):
        """Creates SQL needed to insert a new pstats data row into db."""
        # Create list of field names/values.
        field_vals = []
        for col in self.COLS_NUMERIC:
            fv = udb.FieldValue(col, row[col], dtype=udb.DT.NUMBER)
            field_vals.append(fv)
        for col in self.COLS_STR:
            field_val = udb.FieldValue(col, row[col], dtype=udb.DT.STR)
            field_vals.append(field_val)
        for col in self.COLS_BOOL:
            field_val = udb.FieldValue(col, row[col], dtype=udb.DT.BOOL)
            field_vals.append(field_val)
        for col in self.COLS_DATE:
            field_val = udb.FieldValue(col, row[col], dtype=udb.DT.DATE)
            field_vals.append(field_val)
        return udb.create_insert_query(field_vals, 'playerstats')

    def create_queries (self):
        """Prepares list of SQL queries to update playerstats with new data."""
        df = self._get_new_formatted_pstats()
        self.df = df
        # Ensure player name strings properly formatted.
        for fld in self.COLS_CLEAN_STR:
            df[fld] = df[fld].apply(udb.clean_str_for_sql)
        df['sql'] = df.apply(self._create_row_sql, axis=1)
        queries = df['sql'].tolist()
        return queries

    def date_range (self):
        """Produces tuple with information on game dates spanned by sql queries.

        Returns:
            num_dates (int): Number of unique game dates in data.
            min_date (datetime): Earliest game date in data.
            max_date (datetime): Latest game date in data.
        """
        try:
            dates = self.df['gd'].unique().tolist()
        except:
            return 0, None, None
        num_dates = len(dates)
        min_date = min(dates)
        max_date = max(dates)
        return num_dates, min_date, max_date


class PStatsTSIDUpdate:
    """Adds tsid to playerstats rows with a NULL tsid.

    Attributes:
        pstats (DataFrame): Rows of playerstats that need to be updated.
        tstats (DataFrame): Copy of teamstats from which to figure out the tsid.

    Args:
        db_conn (pyodbc.Connection): Used to create DataFrames.
        season (int): Optional, defaults to module-level `SEASON` variable.
        Specifies the season for the statistics we want to update.
    """

    def __init__ (self, db_conn, season=None):
        """Inits copies of pstats and tstats."""
        # Form sql queries to load playerstats & teamstats.
        if not season:
            season = SEASON
        psql = 'SELECT * FROM playerstats WHERE season = {} AND ' \
               'tsid IS NULL'.format(season)
        tsql = 'SELECT * FROM teamstats WHERE season = {}'.format(season)
        # Load DataFrames.
        self.pstats = pd.read_sql(psql, db_conn)
        self.tstats = pd.read_sql(tsql, db_conn)

    def create_queries (self):
        s = []
        if len(self.pstats.index)==0:
            return s

        for (gid, team), grp in self.pstats.groupby(['gid', 'team']):
            s.append(self._create_sql_tsid(gid, team, grp))

        return s

    def _get_tsid (self, gid, team):
        """Returns ID linking tstats to team / gid group from pstats_data."""
        game = udu.filter_df(self.tstats, {'gid':[gid], 'team':[team]})
        try:
            return game.iloc[0]['id']
        except IndexError:
            raise CriticalDBError('Failed to find tstats record match for: '
                                  '\ngid: {}\nteam: {}'.format(gid, team))

    def _create_sql_tsid (self, gid, team, grp):
        """Returns tsid UPDATE query to be applied to a team / gid group."""
        tsid = self._get_tsid(gid, team)
        rids = grp['id'].tolist()
        in_clause = udb.create_sql_in_clause(rids)
        return 'UPDATE playerstats SET tsid = {} ' \
               'WHERE playerstats.id {}'.format(tsid, in_clause)


class PStatsStarterUpdate:
    """Updates starter field in playerstats for games where there are no
    players flagged as being starters.

    Attributes:
        pstats_data (list of tuple): (gid, team, grp) for each game/team
            GroupBy that needs to be updated with starters.
        tstats (DataFrame): Copy of teamstats.
    """

    def __init__ (self, db_conn):
        """Inits pstats_data and tstats.

        Args:
            db_conn (pyodbc.Connection): Used to create DataFrames.
        """
        self.pstats_data = self._get_pstats_needing_starters(db_conn)
        self.tstats = pd.read_sql(
              'SELECT * FROM teamstats WHERE season = {}'.format(SEASON),
              db_conn)

    @staticmethod
    def _get_pstats_needing_starters (db_conn):
        df = pd.read_sql(
              'SELECT * FROM playerstats WHERE season = {}'.format(SEASON),
              db_conn)
        relevant_grps = []
        for (gid, team), grp in df.groupby(['gid', 'team']):
            starter_count = len(grp[grp['starter']==True].index)
            if starter_count==0:
                relevant_grps.append((gid, team, grp))
        return relevant_grps

    def create_queries (self):
        s = []
        for (gid, team, grp) in self.pstats_data:
            # Get list of starters from tstats for this team/game.
            tstats = udu.filter_df(self.tstats, {'gid':[gid], 'team':[team]},
                                   as_series=True)
            starters = [tstats['start1'], tstats['start2'], tstats['start3'],
                        tstats['start4'], tstats['start5']]
            # Create field on pstats group to determine if player started.
            is_starter = lambda x:x['player'] in starters
            grp['started'] = grp.apply(is_starter, axis=1)
            grp = grp[grp['started']==True]
            starter_ids = grp['id'].tolist()
            s.append(self._create_sql_update_query(starter_ids))
        return s

    @staticmethod
    def _create_sql_update_query (player_ids):
        in_clause = udb.create_sql_in_clause(player_ids)
        return 'UPDATE playerstats SET starter = True ' \
               'WHERE playerstats.id {}'.format(in_clause)

    @property
    def no_new_data (self):
        """Returns True if there is no data needing to be updated."""
        return len(self.pstats_data)==0


#####################################################################

def create_log_path (base_file_name):
    """Creates file path to which we'll save a DBWrapper log.

    To create the path, today's date is appended to base_file_name and saved
    into the DF Experiments folder.
    """
    date_str = datetime.date.today().strftime("%d-%B")
    return '{}{} {}'.format(dfs.Folders.DFExperiments, base_file_name, date_str)


def add_new_pstats ():
    db = dfm.create_nba_dbwrapper()
    min_gd = db.query_scalar('SELECT MAX(gd) FROM playerstats')
    max_gd = datetime.datetime.today()
    queries = NewPStats(min_gd, max_gd).create_queries()

    db_update = udb.DBUpdate(db)
    log_path = create_log_path('DB Insert - PStats')
    db_update.update(queries, log_path)


def add_new_tstats ():
    db = dfm.create_nba_dbwrapper()
    min_gd = db.query_scalar('SELECT MAX(gd) FROM teamstats')
    max_gd = datetime.datetime.today()
    queries = NewTStats(min_gd, max_gd).create_queries()

    db_update = udb.DBUpdate(db)
    log_path = create_log_path('DB Insert - TStats')
    db_update.update(queries, log_path)


def update_pstats_tsid (season=None):
    db = dfm.create_nba_dbwrapper()
    queries = PStatsTSIDUpdate(db.conn, season).create_queries()

    db_update = udb.DBUpdate(db)
    if season:
        log_path = create_log_path('DB Update - tsid - {}'.format(season))
    else:
        log_path = create_log_path('DB Update - tsid')
    db_update.update(queries, log_path)


def update_pstats_starter ():
    db = dfm.create_nba_dbwrapper()
    queries = PStatsStarterUpdate(db.conn).create_queries()

    db_update = udb.DBUpdate(db)
    log_path = create_log_path('DB Update - starter')
    db_update.update(queries, log_path)


def add_new_stats_to_db (add_pstats, add_tstats, ud_tsid, ud_starter):
    """Central method to update database."""
    if add_pstats:
        log.s('Adding new PStats')
        try:
            add_new_pstats()
        except FileNotFoundError as e:
            print(str(e))
        finally:
            log.e(msg='')

    if add_tstats:
        log.s('Adding new TStats')
        try:
            add_new_tstats()
        except FileNotFoundError as e:
            print(str(e))
        finally:
            log.e(msg='')

    if ud_tsid:
        log.s('Updating PStats tsid')
        update_pstats_tsid()
        log.e(msg='')

    if ud_starter:
        log.s('Updating PStats starter')
        update_pstats_starter()
        log.e(msg='')


if __name__=='__main__':
    add_new_stats_to_db(add_pstats=True, add_tstats=True, ud_tsid=True,
                        ud_starter=True)
