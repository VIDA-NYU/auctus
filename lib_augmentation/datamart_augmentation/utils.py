from dateutil.parser import parse
import distance
import numpy as np


def compute_levenshtein_sim(str1, str2):
    """
    Computer the Levenshtein Similarity between two strings using 3-grams.
    """
    if len(str1) < 3:
        str1_set = [str1]
    else:
        str1_set = [str1[i:i + 3] for i in range(len(str1) - 2)]

    if len(str2) < 3:
        str2_set = [str2]
    else:
        str2_set = [str2[i:i + 3] for i in range(len(str2) - 2)]

    return 1 - distance.nlevenshtein(str1_set, str2_set, method=2)


def conv_float(x):
    try:
        return float(x)
    except Exception:
        return np.nan


def conv_int(x):
    try:
        return int(x)
    except Exception:
        return np.nan


def conv_datetime(x):
    try:
        return parse(x)
    except Exception:
        return np.nan
