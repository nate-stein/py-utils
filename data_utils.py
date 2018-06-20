"""
General utils for retrieving, cleaning and writing data.
"""

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=DeprecationWarning)
import collections
from itertools import chain
from typing import List, Iterable, Sequence
import numpy as np
from pandas import DataFrame, Series
import pandas as pd
import statsmodels.api as sm
from statsmodels.regression.linear_model import RegressionResults
import numbers

import dfs.utils.io as uio


#####################################################################

class DataUtilsError(Exception):
    """Encapsulates errors encountered in this module."""

    def __init__ (self, method, info=None):
        msg = 'Error encountered in data_utils.py method: {0}.'.format(method)
        if info is not None:
            msg = msg + ' ' + info
        Exception.__init__(self, msg)

        self.method = method
        self.info = info


#####################################################################
# Saving data.

def save_reg_model_to_excel (model, path):
    """Creates DF out of model results and saves it to Excel.

    Args:
        model (RegressionResults): Fitted OLS model.
    """
    if model is None:
        return
    df = create_df_from_reg_model_results(model)
    uio.save_df_to_excel(df, path)


#####################################################################
# DataFrame utils.

def flatten_sequence_col (df, column) -> DataFrame:
    """Unpacks what is assumed to be a sequence in column, creating t new
    rows for each original row where t is the number of elements contained in
    the original df_entries's column (the original column value is not kept).

    Examples:
        df_entries with orig_row:
            { 'id': 4, 'X1': 10, 'X2': ['A', 'B'] }
        flatten_sequence_col(df_entries, 'X2') would replace orig_row with the
        following two rows:
            { 'id': 4, 'X1': 10, 'X2': 'A' }
            { 'id': 4, 'X1': 10, 'X2': 'B' }
    """
    idxs = []
    new_vals = []
    for i, orig_list in enumerate(df[column]):
        for value in orig_list:
            idxs.append(i)
            new_vals.append(value)
    new_df = df.iloc[idxs, :].copy()
    new_df[column] = new_vals
    return new_df


def filter_df (df, criteria, as_series=False):
    """Filter DataFrame to those rows whose columns meet ALL of the criteria.

    Args:
        df (DataFrame): Data to filter.
        criteria (dict): Criteria to filter by.
        as_series (bool): Optional, default False. If True, then a matching
            DataFrame that consists of only one row will be returned as a
            Series.

    Returns:
        match (DataFrame or Series): Slice of df_entries meeting all criteria.
    """
    match = df[df.isin(criteria).sum(1) == len(criteria.keys())]
    if as_series:
        if len(match.index) == 1:
            return match.iloc[0]
    return match


def remove_outliers (df, fld, top=0, bottom=0):
    """Removes outliers from data.

    Args:
        df (DataFrame): Data.
        fld (str): Field from which to determine outliers.
        top (float | int): Optional. If float, then rows with the
        `top`-percentage highest value in `fld` will be removed. If int,
        then the `top`-highest records (based on value in `fld`) will be
        removed.
        bottom (float | int): Optional. Works same as `top` except for the
        lowest values in field.

    Returns:
        DataFrame: Data with outliers removed.
    """
    if top >= 1 or bottom >= 1:
        # Assume caller wants to remove absolute number of outliers (not pct).
        top = int(top)
        bottom = int(bottom)
        return __remove_outliers_absolute(df, fld, top, bottom)

    bottom_thresh = df[fld].quantile(bottom)
    upper_thresh = df[fld].quantile(1 - top)
    return df[(df[fld] >= bottom_thresh) & (df[fld] <= upper_thresh)]


def __remove_outliers_absolute (df, fld, top, bottom):
    """Removes outliers based on absolute numbers of high and low outliers."""
    if (top + bottom) > len(df.index):
        err = '(top + bottom) to remove > size of DF.'
        raise DataUtilsError('__remove_outliers_absolute', err)

    df = df.copy(deep=True)
    if top != 0:
        for q in range(0, top):
            drop_id = df[fld].idxmax()
            df.drop(drop_id, inplace=True)
    if bottom != 0:
        for q in range(0, bottom):
            drop_id = df[fld].idxmin()
            df.drop(drop_id, inplace=True)
    return df


def extract_top_rows (df, fld, n) -> DataFrame:
    """Returns DF containing only those rows with the n-highest fld value."""
    df.sort_values(fld, ascending=False, inplace=True)
    return df[:n]


def get_highest_value_keys (df, fld, n, return_key=None) -> List:
    """Returns ids pertaining to rows whose _value is among n-highest in
    given fld.
    :param  return_key: If provided, then instead of returning the index for
    rows with the n-highest field, it will return the return_key field for
    those rows.
    """
    n_df = extract_top_rows(df, fld, n)
    if return_key is not None:
        return n_df[return_key].tolist()
    return n_df.index.values.tolist()


def get_top_corr (corr_matrix, n, mode='pos'):
    """Returns list of n-highest correlation pairs in corr_matrix,
    where first element is tuple of index _value pairs and the second element
    is the correlation (for easy reference).
    :param  mode: Default 'pos' will return the largest correlations (i.e. most
            positive or lease negative).
            neg: will return lowest correlations.
            abs: will return correlations with largest magnitude (whether
            positive or
            negative).

    Example return list:
        [
        [('B', 'D'), 0.9],
        [('Csim', 'E'), 0.88]
        ]
    """

    def get_redundant_pairs (df):
        """Get diagonal and lower triangular pairs of correlation matrix."""
        result = set()
        cols = df.columns
        for i in range(0, df.shape[1]):
            for j in range(0, i + 1):
                result.add((cols[i], cols[j]))
        return result

    if mode == 'abs':
        corr_matrix = corr_matrix.abs()

    # Create Series with new level of column labels consisting of tuples
    # created by
    # joining the index (row) _value with the column _value.
    au_corr = corr_matrix.unstack()

    # Then drop the redundant labels so our result only consists of unique
    # pairs.
    labels_to_drop = get_redundant_pairs(corr_matrix)
    au_corr = au_corr.drop(labels=labels_to_drop).sort_values(ascending=False)[
              0:n]

    # Store each pair and corresponding correlation in resulting list.
    pairs = au_corr.index.values
    result = []
    for pair in pairs:
        result.append([pair, au_corr[pair]])
    return result


#####################################################################
# OLS model.

def fcst_with_model (
      model: RegressionResults or DataFrame, live_data: Series,
      sigma=0) -> float:
    """Returns projection by taking sumproduct of dlive and coefficients
    from model.
    :param  model: Fitted OLS model whose coefficients we will use to compute
            prediction OR DataFrame formed from fitted OLS model containing that
            information.
    :param  live_data: Contains predictor variable values that will be
    multiplied by
            the model coefficients.
    :param  sigma: Standard error(s) to use for each beta. For example,
    if we were
            looking at 'bear' case we could use sigma = -1.
    """
    # Extract betas and standard errors from model.
    if isinstance(model, DataFrame):
        betas = model['beta'].to_dict()
        std_errs = model['stderr'].to_dict()
    else:
        df = create_df_from_reg_model_results(model)
        betas = df['beta'].to_dict()
        std_errs = df['stderr'].to_dict()

    # Initialize result to intercept; then delete it, or it would get
    # double-counted in
    # subsequent loop.
    result = betas['intercept'] + (sigma*std_errs['intercept'])
    del betas['intercept']
    try:
        for variable, coeff in betas.items():
            coeff += (sigma*std_errs[variable])
            effect = coeff*live_data[variable]
            result += effect
        return result
    except:
        raise DataUtilsError('fcst_with_model',
                             'Failed computing sumproduct of betas and '
                             'dlive data.')


def fit_ols_model (df, x, y, min_obs=0, intcp=0,
                   dropna=True) -> RegressionResults:
    """Returns RegressionResults after fitting statsmodels.api.OLS on predictor
    variables xvars and dependent variable yvar.
    :param  df: Contains data with which to train the model.
    :param  min_obs: Minimum observations from df_entries from which to train
    OLS
    model before
            raising DataUtilsError.
    :param  intcp: Model intercept.
    :param  dropna: If True, all rows where any or all of the data are
    missing from the
            x- or y-var are dropped before creating model.
    """
    # Remove fields containing faulty data.
    # Before doing this, however, filter df_entries down to only those fields used in
    #  the
    # regression model to avoid dropping records with faulty values in
    # irrelevant fields.
    xvar, yvar, params = __create_params_list(x, y)
    df = df[params].copy(deep=True)
    if dropna:
        df.dropna(how='any', inplace=True)

    if len(df.index) == 0:
        raise DataUtilsError('fit_ols_model',
                             'Training DF empty after dropping NA.')

    # Extract X & Y variables.
    try:
        X = df[xvar].astype(float)
        Y = df[yvar].copy()
    except:
        raise DataUtilsError('fit_ols_model',
                             'Extracting xy-variables from DF.')

    X['intercept'] = intcp

    try:
        model = sm.OLS(Y, X).fit()
    except:
        raise DataUtilsError('fit_ols_model',
                             'Fitting model: sm.OLS(Y, X).fit()')

    # Raise error if number of observations used in creating model doesn't
    # meet minimum.
    # Obs count = degree of freedom + residual degree of freedom + 1 (if int.
    #  <> 0)
    obs = model.df_model + model.df_resid
    if intcp != 0:
        obs += 1
    if obs < min_obs:
        raise DataUtilsError('fit_ols_model',
                             'Insufficient obs: 0.'.format(obs))
    return model


def __create_params_list (x, y):
    """Returns 3-tuple consisting of (x vars, y vars, x + y vars list) where
    each element of the tuple is a list."""
    if isinstance(x, str):
        x_var = [x]
    else:
        x_var = x
    if isinstance(y, str):
        y_var = [y]
    else:
        y_var = y

    try:
        params = x_var + y_var
    except TypeError:
        params = list(x)
        params.append(y)
    return x_var, y_var, params


def create_df_from_reg_model_results (model: RegressionResults) -> DataFrame:
    """Returns DF made from regression model results where index equals the
    independent variable names and columns are the beta, standard error,
    t- and p-values.
    """
    try:
        df = pd.concat((model.params, model.bse, model.tvalues, model.pvalues),
                       axis=1)
    except:
        raise DataUtilsError('create_df_from_reg_model_results',
                             'Concatenating model results into DF.')
    try:
        df.columns = ['beta', 'stderr', 't_value', 'prob_t']
    except:
        raise DataUtilsError('create_df_from_reg_model_results',
                             'Setting DF column names.')
    return df


#####################################################################
# Collections / arrays.

def contains_elements (a, b):
    """Returns True if sequence a contains all elements in sequence b."""
    a, b = set(a), set(b)
    section = list(a.intersection(b))
    return len(section) == len(b)


def dicts_equal (a: dict, b: dict):
    """Returns True if a and b share same keys and corresponding values."""
    a_keys = set(a.keys())
    b_keys = set(b.keys())
    if not sequences_equal(a_keys, b_keys, order=False):
        return False

    for key in a_keys:
        if a[key] != b[key]:
            return False
    return True


def flatten (iterable):
    """Returns elements in iterable flattened by one hierarchy level down.

    Examples:
        flatten( ( ('A','B','Csim'), ('D', 'E') ) = ['A','B','Csim','D','E']
    """
    return [x for x in chain.from_iterable(iterable)]


def common_elements (x):
    """Finds elements common to all sequences in x.
    
    Args:
        x (list[Iterable]): Iterables in which to look for common elements.
    
    Returns:
        set: Unique elements common to all sequences in x.
    """
    result = set(x[0])
    if len(x) == 1:
        return result
    for seq in x[1:]:
        result.intersection_update(seq)
    return result


def all_elements_equal (q):
    """Returns True if all elements in sequence q are the same."""
    return q.count(q[0]) == len(q)


def is_collection (q):
    """Returns True if q is an iterable that is not a string """
    if isinstance(q, str):
        return False
    if hasattr(q, '__len__'):
        return True
    if isinstance(q, (collections.Sequence, np.ndarray)):
        return True
    return False


def sequences_equal (a, b, order=False):
    """Determines if both sequences contain the same elements.

    Args:
        a (Sequence): First sequence.
        b (Sequence): Second sequence.
        order (bool): Optional, default False. If True, then all elements must
        also be in the same order for function to return True.

    Returns:
        bool: True if both sequences contain the same elements.
    """
    if order:
        return np.array_equal(a, b)
    return collections.Counter(a) == collections.Counter(b)


#####################################################################
# Data validation / computation.

def is_number (x):
    return isinstance(x, numbers.Number)


def on_err_return_na (f):
    """Function decorator to return an NaN if exception is encountered."""

    def wrapper (*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            return np.NaN

    return wrapper


def on_err_return_same (f):
    """Function decorator to return input if exception is encountered."""

    def wrapper (x):
        try:
            return f(x)
        except:
            return x

    return wrapper
