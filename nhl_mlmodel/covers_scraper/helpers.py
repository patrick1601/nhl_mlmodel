def remove_duplicates(x: list) -> list:
    """
    takes a list and removes duplicates from that list

    ...

    Parameters
    ----------
    x: list
        list from which duplicates will be removed

    Returns
    -------
    list
        list with duplicates removed
    """
    return list(dict.fromkeys(x))