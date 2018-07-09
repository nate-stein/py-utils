"""
Methods to perform data integrity checks on DFS data sources and reconcile
data form different sources.
"""
import operator
import re
from typing import List, Tuple

import dfs
import dfs.utils.main as dfm
from nltk.metrics.distance import edit_distance
import pandas as pd


#####################################################################

class NameConverter(object):
    """Converts names from various sources into DB-friendly names.

    Attributes:
        known_pairs (dict): Maps various versions of a player's name (e.g.,
        different spellings encountered in various sources) to the
        DB-recognized version.
        known_missing (set): Players known to be missing from PStats.
        known (set): Recognized names in database (i.e., "approved" names).
        unhandled_names (set): Collection of names not captured by existing
        name lists. Such names are added in calls to `clean()` when the name
        is not found in one of the approved exception sets.

    Args:
        excl_teams (bool): Optional, default False. If True, then all
        forms of team names are considered "known" names, i.e., an error
        will not be raised if a team name is encountered even though it
        is not a player's name.
        raise_exc (bool): Optional, default True. If True, then `clean()`
        method raises PMissingError when unhandled name encountered.
    """

    def __init__ (self, excl_teams=False, raise_exc=True):
        """Inits key lists used in determining whether a name is known."""
        self.known_pairs = load_same_name_pairs()
        self.known_missing = load_known_missing()
        self.known = load_known_names()
        self.unhandled_names = set()
        self.raise_exc = raise_exc
        if excl_teams:
            teams = load_all_team_representations()
            for t in teams:
                self.known.add(t)

    def clean (self, name):
        if name in self.known_pairs:
            return self.known_pairs[name]
        if name in self.known_missing or name in self.known:
            return name
        # Add name to `unhandled_names` and raise error.
        self.unhandled_names.add(name)
        if self.raise_exc:
            msg = '{} not found in data, associated pairs, or known ' \
                  'to be missing.'.format(name)
            raise dfm.PMissingError('clean', name, msg)

    def is_problematic (self, name):
        if name in self.known_pairs:
            return True, 'Should be {}.'.format(self.known_pairs[name])
        if name in self.known_missing:
            return False, 'Known to be missing from PStats.'
        if name in self.known:
            return False, 'In PStats.'
        return True, 'Not in PStats, no associated name, and not known to be ' \
                     'missing.'

    @property
    def some_names_missing (self):
        """Returns True if some names encountered that were not in any of the
        exception sets.
        """
        return len(self.unhandled_names)!=0


def load_known_names ():
    """Returns set of names contained in PStats 2017-2018 seasons."""
    sql = 'SELECT player FROM playerstats WHERE season in (2017, 2018)'
    df = dfm.load_bespoke(sql)
    return set(df['player'].tolist())


def load_same_name_pairs ():
    """Returns dict mapping various names to their DB-approved version."""
    df = pd.read_excel(dfs.Paths.DataNames, sheet_name='Conversions')
    df.set_index('old_name', drop=True, inplace=True)
    return df['new_name'].to_dict()


def load_known_missing ():
    """Returns players that are known to be missing from PStats."""
    df = pd.read_excel(dfs.Paths.DataNames, sheet_name='Known_Missing')
    return set(df['player'].unique().tolist())


def load_all_team_representations () -> List[str]:
    """Returns all different references to active teams."""
    sql = 'SELECT nba_code, short_name, full_name, mascot FROM teams ' \
          'WHERE active=True'
    df = dfm.load_bespoke(sql)
    results = []
    for field in ['nba_code', 'short_name', 'full_name', 'mascot']:
        results = results + df[field].tolist()
    return list(set(results))


def load_known_similar_names () -> List[Tuple]:
    """Returns list of 2-tuples for names that are close to each other in
    terms of Levenshtein distance but that are indeed different.
    """
    df = pd.read_excel(dfs.Paths.DataNames, sheet_name='Name_Log')
    func_pair = lambda x:(x['name1'], x['name2'])
    df['name_pair'] = df.apply(func_pair, axis=1)
    return df['name_pair'].tolist()


#####################################################################
class AliasWizard(object):
    """Container class for utils pertaining to nicknames (aliases).

    DataFrame containing name:nickname mapping is loaded into memory upon
    inception at the class level since it shouldn't change during program
    execution.
    """
    df = pd.read_excel(dfs.Paths.DataNicknames)
    names = set(df['name'].unique())
    nicknames = set(df['nickname'].unique().tolist())

    def get_aliases (self, name):
        """Returns list of aliases, which can be regular names if name is a
        nickname or a list of nicknames if name is a regular name.

        Args:
            name (str): First name (whether for standard name or nickname).
        """
        if name in self.names:
            df = self.df[self.df['name']==name]
            return df['nickname'].unique().tolist()
        elif name in self.nicknames:
            df = self.df[self.df['nickname']==name]
            return df['name'].unique().tolist()
        return None

    def has_aliases (self, name: str) -> bool:
        """Returns True if name is found among class names or nicknames."""
        if name in self.names:
            return True
        elif name in self.nicknames:
            return True
        return False


#####################################################################
class Name:
    """Data container for a player name with RegEx internals that decompose a
    name into its component parts.
    """
    # Regex objects to match different name scenarios.
    # Breaks down enter name into first, last & suffix.
    __rgx_full_name = re.compile(
          r'^(?P<first>.+?)\s(?P<last>[^\s,]+)(,?\s(?P<suffix>['
          r'JS]r\.?|III?|IV|V))?$')
    # Abbreviated first names w/ periods (e.g. C.J.).
    __rgx_abbr_periods = re.compile(r'[A-Z]\.[A-Z]\.')
    # Abbreviated first names w/out periods (e.g. CJ).
    __rgx_abbr_no_periods = re.compile(r'[A-Z][A-Z]')

    def __init__ (self, full_name: str):
        self.full_name = full_name.strip()
        m = self.__rgx_full_name.match(self.full_name)
        self.first = m.group('first')
        self.last = m.group('last')
        self.suffix = m.group('suffix')


def create_name_objects (names):
    """Creates Name objects from list of strings.

    Args:
        names (list[str]): Names to clean.

    Returns:
        list[Name]: Name objects created from string names.
    """
    s = []
    for name in names:
        try:
            s.append(Name(name))
        except:
            pass
    return s


class AbbreviatedNameWizard:
    """Finds matches between abbreviated names, such as those used in ESPN's
    box scores and authoritative reference of players' full names.

    Attributes:
        raw_names (list[str]): Pre-vetted names (i.e., those in the database).
        name_objs (list[Name]): Name objects initialized from raw_names.
        used_levenshtein (dict): Used to log cases where, as a get_final resort,
        we use Levenshtein distance to find the best match among full names
        for the abbreviated name (as opposed to first initial and last name).
    """

    def __init__ (self, universe):
        self.raw_names = universe
        self.name_objs = create_name_objects(universe)
        self.used_levenshtein = {}

    @staticmethod
    def __get_first_init_last_name (abbrev):
        """Creates (first initial, last name) tuple from abbreviated name."""
        first_initial = abbrev[0]
        last_name = abbrev[3:]
        return first_initial, last_name

    def full_name (self, abbrev):
        """Returns matching full name from universe for abbreviated name.

        If no match is found using first initial and last name, then returns the
        element from raw_names with the smallest Levenshtein distance to the
        abbreviated name.

        Args:
            abbrev (str): Abbreviated name.
        """
        first_init, last_name = self.__get_first_init_last_name(abbrev)
        for name in self.name_objs:
            if name.last!=last_name:
                continue
            if name.first[0]==first_init:
                return name.full_name

        # Use edit distance as last resort and log it.
        edit_distances = name_edit_distances(abbrev, self.raw_names)
        result = edit_distances[0][0]
        self.used_levenshtein[abbrev] = result
        return result

    @property
    def used_levenshtein_count (self):
        return len(self.used_levenshtein)


class MostSimilarNameWizard:
    """Finds most similar name among universe of pre-vetted names."""

    # Regex objects to match different name scenarios.
    # Breaks down enter name into first, last & suffix.
    __rgx_full_name = re.compile(
          r'^(?P<first>.+?)\s(?P<last>[^\s,]+)(,?\s(?P<suffix>['
          r'JS]r\.?|III?|IV|V))?$')
    # Abbreviated first names w/ periods (e.g. C.J.).
    __rgx_abbr_periods = re.compile(r'[A-Z]\.[A-Z]\.')
    # Abbreviated first names w/out periods (e.g. CJ).
    __rgx_abbr_no_periods = re.compile(r'[A-Z][A-Z]')

    def __init__ (self, names: List[str]):
        self.universe = names
        self.alias_wizard = AliasWizard()

    def __break_down_name_components (self, full_name):
        """Saves full name to this instance and breaks down the full name
        into its components.
        """
        self.name = full_name
        m = self.__rgx_full_name.match(self.name)
        self.first = m.group('first')
        self.last = m.group('last')
        self.suffix = m.group('suffix')

    def solve (self, full_name: str):
        if full_name in self.universe:
            return full_name

        self.__break_down_name_components(full_name)

        # abbreviated first names w/out periods (e.g. CJ)
        if self.__rgx_abbr_no_periods.match(self.first):
            new_name = self.first[0] + '.' + self.first[1] + '.' + ' ' + \
                       self.last
            if new_name in self.universe:
                return new_name
            raise dfm.PMissingError('solve', full_name)

        # abbreviated first names w/ periods (e.g. C.J.)
        if self.__rgx_abbr_periods.match(self.first):
            new_name = self.first.replace('.', '') + ' ' + self.last
            if new_name in self.universe:
                return new_name
            raise dfm.PMissingError('solve', full_name)

        # remove suffix if player has one
        if self.suffix is not None:
            new_name = self.first + ' ' + self.last
            if new_name in self.universe:
                return new_name
            raise dfm.PMissingError('solve', full_name)

        # apostrophes in first or last names
        if "'" in self.first:
            new_name = self.first.replace("'", '') + ' ' + self.last
            if new_name in self.universe:
                return new_name
        if "'" in self.last:
            new_name = self.first + ' ' + self.last.replace("'", '')
            if new_name in self.universe:
                return new_name

        # compound surname with hyphen
        if '-' in self.last:
            last_names = self.last.split('-')
            for n in last_names:
                new_name = self.first + ' ' + n
                if new_name in self.universe:
                    return new_name

        # first name alias
        aliases = self.alias_wizard.get_aliases(self.first)
        if aliases is not None:
            for alias in aliases:
                new_name = alias + ' ' + self.last
                if new_name in self.universe:
                    return new_name

        raise dfm.PMissingError('solve', full_name)


def name_edit_distances (ref_name, other_names):
    """Computes Levenshtein distance (LD) between name and other_names.

    Args:
        ref_name (str): Reference name against which to compare each element
        in other_names.
        other_names (list): Names that will be compared to ref_name.

    Returns:
        s (list): Each element a tuple containing a name from other_names and
        the LD between it and ref_name. Sorted so that s[0] corresponds to
        the name with the shortest LD.
    """
    s = []
    for name in other_names:
        s.append((name, edit_distance(name, ref_name)))
    s.sort(key=operator.itemgetter(1))
    return s
