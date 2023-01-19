from astropy.time import Time

def parse_date(date_str):
    if type(date_str) == Time:
        return date_str
    elif type(date_str) == str:
        return Time(date_str)
    else:
        raise ValueError("Input date must be either a string or a astropy Time object")
