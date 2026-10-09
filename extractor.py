import requests

def extract_311_page(api_url, limit, offset):

    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "created_date DESC"
    }

    response = requests.get(
        api_url,
        params=params,
        timeout=120
    )

    response.raise_for_status()

    return response.json()

def extract_311_incremental_page(
        api_url,
        lower_watermark,
        upper_watermark,
        limit,
        offset
):

    where_condition = (
        f"("
        f"created_date > '{lower_watermark}' "
        f"AND created_date <= '{upper_watermark}'"
        f") "
        f"OR "
        f"("
        f"resolution_action_updated_date > '{lower_watermark}' "
        f"AND resolution_action_updated_date <= '{upper_watermark}'"
        f")"
    )

    print("WHERE CONDITION:")
    print(where_condition)

    params = {
        "$limit": limit,
        "$offset": offset,
        "$where": where_condition,
        "$order": "unique_key ASC"
    }

    response = requests.get(
        api_url,
        params=params,
        timeout=120
    )

    response.raise_for_status()

    return response.json()