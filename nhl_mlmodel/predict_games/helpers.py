def convert_minutes(min):
    """
    convert goalie string minutes in form xx:xx to just minutes
    ...

    Parameters
    ----------
    min: str
        minutes string in form xx:xx

    Returns
    -------
    minutes_played: float
        minutes string converted to numerical minutes
    """
    try:
        split_min = min.split(':')
        minutes = int(split_min[0])
        seconds = int(split_min[1])
        minutes_played = minutes + seconds/60
        return minutes_played
    except AttributeError:
        pass

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