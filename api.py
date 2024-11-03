# ns_stations_dataframe.py

import requests
import pandas as pd


def get_ns_stations(
    api_key: str,
    q: str = None,
    include_non_plannable: bool = False,
    country_codes: str = None,
    limit: int = None,
):
    """
    Fetches NS station data using the NS API with optional filters.

    Parameters:
    - api_key (str): NS API key for authentication.
    - q (str): Query string to search for stations by name.
    - include_non_plannable (bool): Whether to include non-plannable stations.
    - country_codes (str): Comma-separated country codes to filter stations.
    - limit (int): Maximum number of stations to return.

    Returns:
    - dict: JSON response containing NS station data if the request is successful.
    - None: If the request fails.
    """
    url = "https://gateway.apiportal.ns.nl/nsapp-stations/v3"
    headers = {
        "Ocp-Apim-Subscription-Key": api_key,
        "Content-Type": "application/json",
    }

    params = {}
    if q:
        params["q"] = q
    if include_non_plannable:
        params["includeNonPlannableStations"] = "true"
    if country_codes:
        params["countryCodes"] = country_codes
    if limit:
        params["limit"] = limit

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")

    return None


def ns_data_to_dataframe(data):
    """
    Converts NS API station data to a pandas DataFrame, handling missing fields.

    Parameters:
    - data (dict): NS station data from the API.

    Returns:
    - pd.DataFrame: DataFrame containing structured NS station data.
    """
    station_list = []
    for station in data.get("payload", []):
        station_info = {
            "uicCode": station.get("id", {}).get("uicCode", None),
            "evaCode": station.get("id", {}).get("evaCode", None),
            "cdCode": station.get("id", {}).get("cdCode", None),
            "code": station.get("id", {}).get("code", None),
            "stationType": station.get("stationType", None),
            "name_long": station.get("names", {}).get("long", None),
            "name_medium": station.get("names", {}).get("medium", None),
            "name_short": station.get("names", {}).get("short", None),
            "name_festive": station.get("names", {}).get("festive", None),
            "synonyms": station.get("names", {}).get("synonyms", []),
            "lat": station.get("location", {}).get("lat", None),
            "lng": station.get("location", {}).get("lng", None),
            "tracks": station.get("tracks", []),
            "hasKnownFacilities": station.get("hasKnownFacilities", None),
            "availableForAccessibleTravel": station.get(
                "availableForAccessibleTravel", None
            ),
            "hasTravelAssistance": station.get("hasTravelAssistance", None),
            "areTracksIndependentlyAccessible": station.get(
                "areTracksIndependentlyAccessible", None
            ),
            "isBorderStop": station.get("isBorderStop", None),
            "country": station.get("country", None),
            "radius": station.get("radius", None),
            "approachingRadius": station.get("approachingRadius", None),
            "distance": station.get("distance", None),
            "startDate": station.get("startDate", None),
            "endDate": station.get("endDate", None),
            "nearbyMeLocationId_value": station.get("nearbyMeLocationId", {}).get(
                "value", None
            ),
            "nearbyMeLocationId_type": station.get("nearbyMeLocationId", {}).get(
                "type", None
            ),
        }
        station_list.append(station_info)

    return pd.DataFrame(station_list)


if __name__ == "__main__":
    api_key = "a9dc5c210d1d4b4392769ca647458691"

    # Fetch the NS station data
    data = get_ns_stations(api_key, country_codes="NL")

    # Convert to DataFrame and display
    if data:
        df = ns_data_to_dataframe(data)
        print("NS Stations DataFrame:")
        print(df)
    else:
        print("Failed to retrieve NS Stations data.")
