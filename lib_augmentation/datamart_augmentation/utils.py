import distance


class AugmentationError(ValueError):
    """Error during augmentation.
    """


def compute_levenshtein_sim(str1, str2):
    """
    Computer the Levenshtein Similarity between two strings using 3-grams, if one string
    is not contained in the other.
    """

    if str1 in str2 or str2 in str1:
        return 1

    if len(str1) < 3:
        str1_set = [str1]
    else:
        str1_set = [str1[i:i + 3] for i in range(len(str1) - 2)]

    if len(str2) < 3:
        str2_set = [str2]
    else:
        str2_set = [str2[i:i + 3] for i in range(len(str2) - 2)]

    return 1 - distance.nlevenshtein(str1_set, str2_set, method=2)
