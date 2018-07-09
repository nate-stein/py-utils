"""
Testing the integrity of the data in the database through various checks.
"""

import dfs
import dfs.db.qual_ctrl as dqc
import dfs.utils.data_utils as udu
import dfs.utils.io as uio
import dfs.utils.main as dfm
from pandas import DataFrame
import pandas as pd


class DBIntegrityCheck(object):
    """Logs inconsistencies & other potential problems with NBA database.

    Attributes:
        pstats (DataFrame): Copy of pstats that is inspected.
        tstats (DataFrame): Copy of tstats that is inspected.
        inj (DataFrame): Copy of injuries that is inspected.
        news (DataFrame): Copy of news that is inspected.
        err_log (DataFrame): Where errors are logged.
        name_log (DataFrame): Stores cases where Levenshtein distances
        between names in pstats is less than or equal to `MAX_LEVENSHTEIN`.
        known_sim_names (list[tuple]): Pairs of names that are known to
        have short edit distances but that are, in fact, different (i.e.,
        false positives).

    Args:
        season (int): Optional, defaults to current season. Specific season to
        perform integrity checks for in pstats & tstats.
    """

    # Boundary constraints.
    MAX_OPEN_SPREAD = 25
    MAX_OPEN_PTS = 250
    MIN_OPEN_PTS = 150
    MAX_LEVENSHTEIN = 4

    def __init__ (self, season=None):
        """Loads working copies of PStats / TStats and inits error log."""
        self.pstats = dfm.load_pstats(season)
        self.tstats = dfm.load_tstats(season)
        self.inj = dfm.load_injuries()
        self.news = dfm.load_news()
        self.err_log = pd.DataFrame(columns=['table', 'id', 'info'])
        self.name_log = pd.DataFrame(columns=['name1', 'name2', 'dist'])
        self.known_sim_names = dqc.load_known_similar_names()
        self.name_converter = dqc.NameConverter()

    def run (self):
        """Executes all inspection methods."""
        self.inspect_pstats()
        self.inspect_tstats()
        self.inspect_pstats_names()
        self.inspect_inj_names()
        self.inspect_news_names()

    def inspect_pstats_names (self):
        """Creates DF containing pairs of names and the Levenshtein distance
        between those pairs and stores results in name_log attribute.
        """
        uniq_names = self.pstats['player'].unique().tolist()

        for i, ref_name in enumerate(uniq_names[:-1]):
            other_names = uniq_names[i + 1:]
            distances = dqc.name_edit_distances(ref_name, other_names)
            distances = [x for x in distances if x[1]<self.MAX_LEVENSHTEIN]

            for (name, dist) in distances:
                if self._names_are_equiv(ref_name, name):
                    continue

                row = {'name1':ref_name, 'name2':name, 'dist':dist}
                self.name_log = self.name_log.append(row, ignore_index=True)

        self.name_log.sort_values(by='name1', ascending=False, inplace=True)

    def inspect_inj_names (self):
        inj_names = self.inj['player'].unique().tolist()
        self.inspect_names_in_table(inj_names, 'injuries')

    def inspect_news_names (self):
        news_names = self.news['player'].unique().tolist()
        self.inspect_names_in_table(news_names, 'news')

    def inspect_names_in_table (self, names, table):
        """Logs instances when a name is problematic."""
        for name in names:
            problematic, info = self.name_converter.is_problematic(name)
            if problematic:
                self._log(table, info, name)

    def inspect_pstats (self):
        # Conduct checks on GD / TEAM groups
        for details, grp in self.pstats.groupby(['gd', 'team']):
            gd, team = details
            self.__inspect_team_gd_group(grp, gd, team)

    def inspect_tstats (self):
        for gid, grp in self.tstats.groupby('gid'):
            self.__inspect_gid_grp(grp, gid)

    def _names_are_equiv (self, a, b):
        """Returns True if (a, b) are known as equivalent names."""
        pair = (a, b)
        for known_pair in self.known_sim_names:
            if udu.sequences_equal(pair, known_pair, order=False):
                return True
        return False

    def __inspect_gid_grp (self, grp, gid):
        """Performs checks on gid slice from TStats."""
        # All entries made into error log will have same table and data id so
        # keep them fixed with lambda expression.
        log = lambda x:self._log('TStats', x, 'gid = {}'.format(gid))

        # only two records
        if len(grp.index)!=2:
            log('Row count != 2')
            return

        # set variables to each of the two rows to make subsequent analysis more
        # straightforward
        row1, row2 = grp.iloc[0], grp.iloc[1]

        # only 1 GD
        if row1['gd']!=row2['gd']:
            log('gd count != 1')
            return

        # team / opponent matches up
        if row1['team']!=row2['opp'] or row1['opp']!=row2['team']:
            log('opp/team mismatch')
            return

        # only one home / away team
        if row1['home'] and row2['home']:
            log('2 home teams')
            return

        if not row1['home'] and not row2['home']:
            log('2 away teams')
            return

        # OT periods match
        if row1['ot']!=row2['ot']:
            log('OT periods different')
            return

        # Opening spread figure makes sense on absolute basis and relative
        # figures are concordant
        if row1['open_spread']!=-row2['open_spread']:
            log('open_spread not negative versions of each other')
            return

        if abs(row1['open_spread'])>self.MAX_OPEN_SPREAD:
            log('open_spread > MAX_OPEN_SPREAD')
            return

        # open_pts figures are equivalent and value makes sense
        if row1['open_pts']!=row2['open_pts']:
            log('open_pts different')
            return

        if row1['open_pts']>self.MAX_OPEN_PTS \
              or row1['open_pts']<self.MIN_OPEN_PTS:
            log('open_pts outside boundaries')
            return

    def __inspect_team_gd_group (self, grp, gd, team):
        """Performs checks on gd/team group from PStats."""
        # ID to use when logging any inconsistencies found in this method
        did = 'gd/team grp ({}/{})'.format(gd.strftime('%Y%m%d'), team)

        # 7+ records (players)
        if len(grp.index)<7:
            self._log('PStats', 'Row count < 7', did)
            return

        # 5 starters
        if len(grp[grp['starter']==True].index)!=5:
            self._log('PStats', 'starter count != 5', did)
            return

        # only one GID
        gids = grp['gid'].unique().tolist()
        if len(gids)!=1:
            self._log('PStats', 'gid count != 1', did)
            return

        # only one OPP
        opponents = grp['opp'].unique().tolist()
        if len(opponents)!=1:
            self._log('PStats', 'opp count != 1', did)
            return

        # OPP records share the same GID
        opp_df = self.pstats[(self.pstats['team']==opponents[0]) &
                             (self.pstats['gd']==gd)]
        opp_gids = opp_df['gid'].unique().tolist()
        if gids[0]!=opp_gids[0]:
            self._log('PStats', 'opp gid != gid', did)
            return

        # only one TSID
        tsids = grp['tsid'].unique().tolist()
        if len(tsids)!=1:
            self._log('PStats', 'tsid count != 1', did)
            return

        # TSID matches only one record in TStats
        trow = self.tstats[self.tstats['id']==tsids[0]]
        if len(trow.index)!=1:
            self._log('TStats', 'TStats.id = tsid match count != 1', did)
            return

        # GD & TEAM match expected in TStats record
        tstats_gd = trow.iloc[0]['gd']
        if gd!=tstats_gd:
            self._log('TStats', 'TStats.GD != GD', did)
            return

        tstats_team = trow.iloc[0]['team']
        if tstats_team!=team:
            self._log('TStats', 'TStats.team != team', did)
            return

        # no duplicate records for single player
        uniq_names = grp['player'].unique().tolist()
        if len(grp.index)!=len(uniq_names):
            self._log('PStats', 'Duplicate name', did)
            return

    @property
    def errors (self):
        """Returns number of errors logged in DF."""
        return len(self.err_log.index)

    @property
    def sim_names (self):
        """Returns number of entries in name_log attr."""
        return len(self.name_log.index)

    def _log (self, tbl, info, data_id=None):
        row = {'table':tbl, 'id':data_id, 'info':info}
        self.err_log = self.err_log.append(row, ignore_index=True)


def inspect_db (save_path=None):
    """Runs DBIntegrityCheck and displays results, saving discrepancy log to
    `save_path` if any are found.

    Args:
        save_path (str): Optional. Excel save file path to save errors if
        there are any. Defaults to Experiments folder.
    """
    if save_path is None:
        save_path = dfs.Folders.DFExperiments + 'Database Integrity Check'

    dbc = DBIntegrityCheck()
    dbc.run()
    if dbc.errors==0 and dbc.sim_names==0:
        print('All looks good. No errors or similar player names encountered.')
        return

    # Save error & name log DFs (for convenience).
    # Then let user know where errors were found.
    save_path = uio.clean_excel_path(save_path)
    wks = ['Err_Log', 'Name_Log']
    dframes = [dbc.err_log, dbc.name_log]
    uio.save_dfs_to_excel(dframes, save_path, wks)

    # Print message summarizing results.
    msg = '{} errors, {} similar names encountered.'.format(dbc.errors,
                                                            dbc.sim_names)
    msg += '\n\nResults saved to: {}'.format(save_path)
    print(msg)


if __name__=='__main__':
    inspect_db()
