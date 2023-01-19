from src.harps import drms_client
from src.cmesrc.utils import parse_date

def fetch_jsoc_keys(query_str, keys):
    """
    Fetches a jsoc query with certain keys
    """
    if type(query_str) != str:
        raise TypeError("Query must be a string")

    query = drms_client.query(query_str, key=keys)

    return query

def list_harp_regions(input_date, interval="12m"):
    """
    Returns a dataframe with information about the HARPS regions that were present on-disk at a particular time.
    """

    date = parse_date(input_date)
    formatted_date_str = date.isot.replace("-",".").replace("T","_")

    query_str = f"hmi.sharp_720s[][{formatted_date_str}/{interval}]"

    query = fetch_jsoc_keys(query_str, keys=['HARPNUM', 'LAT_MIN', 'LON_MIN', 'LAT_MAX', 'LON_MAX', 'DATE', 'NOAA_AR', 'NOAA_NUM', 'NOAA_ARS'])

    return query
