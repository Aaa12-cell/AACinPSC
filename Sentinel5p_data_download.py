# https://dataspace.copernicus.eu/news/2023-9-28-accessing-sentinel-mission-data-new-copernicus-data-space-ecosystem-apis
# https://documentation.dataspace.copernicus.eu/APIs/SentinelHub/Process/Examples/S5PL2.html#aer-ai-354-and-388
# https://docs.sentinel-hub.com/api/latest/reference/#tag/async_process/operation/createNewAsyncProcessRequest

import datetime
import pandas as pd
import requests
# Import credentials
from creds import *

def get_keycloak(username: str, password: str) -> str:
    data = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    try:
        r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                          data=data,
                          )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
        )
    return r.json()["access_token"]


keycloak_token = get_keycloak(username, password)

data_collection = "SENTINEL-5P"
aoi = ("POLYGON((14.5847 45.7226,13.4038 45.5415,13.5607 44.936,15.5665 43.5146,15.664 43.0022,16.217 42.3413,18.6382 "
       "42.3402,18.4623 42.6254,16.5556 44.0833,15.9083 44.9078,18.4599 45.021,18.9541 44.733,19.5345 45.1895,18.9001 "
       "46.0192,18.0819 45.8383,16.3968 46.6356,15.5618 46.2256,15.0449 45.5402,14.5847 45.7226))'")

date1 = datetime.date(2020,4,30)
date2 = datetime.date(2018,5,10)

days = [date1 + datetime.timedelta(days=x) for x in range((date2 - date1).days + 1)]

for one_day in days:
    start_date = one_day
    end_date = one_day + datetime.timedelta(days=1)
    print("start date: ", start_date)

    json = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '"
                        f"{data_collection}' and OData.CSC.Intersects(area=geography'SRID=4326;{aoi}) "
                        f"and ContentDate/Start gt {start_date}T00:00:00.000Z and ContentDate/Start lt "
                        f"{end_date}T00:00:00.000Z").json()

    data_df = pd.DataFrame.from_dict(json['value'])
    print()

    # product selection by product name
    # -----------------------------------
    product_name_df = pd.DataFrame(data_df.apply(lambda row: row["Name"][12:20], axis=1))
    product_name_df = product_name_df.rename(columns={0: "Name"})

    product_name_df_res = product_name_df.reset_index()
    product_of_interest_idx = product_name_df_res[
        product_name_df_res["Name"].isin(["_AER_AI_", "_AER_LH_"])].index.to_list()

    data_df_sel = data_df.iloc[product_of_interest_idx]
    product_id_lst = data_df_sel["Id"].to_list()
    product_name_lst = data_df_sel["Name"].to_list()

    for product_id_idx in range(len(product_id_lst)):
        url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products(" + product_id_lst[product_id_idx] + ")/$value"

        session = requests.Session()
        headers = {"Authorization": f"Bearer {keycloak_token}"}
        session.headers.update(headers)
        response = session.get(url, allow_redirects=False)
        with open("product_" + product_name_lst[product_id_idx] + ".zip", "wb") as file:
            print(product_name_lst[product_id_idx])

            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

print()
