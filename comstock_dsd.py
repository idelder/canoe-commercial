"""
Calculates normalised demand distributions for end uses and also by fuel (for annual capacity factors)
Written by Ian David Elder for the CANOE model
"""

from matplotlib import pyplot as pp
from setup import config
import pandas as pd
import numpy as np
import weather_mapping
import os
import utils

comstock_map = pd.read_csv(config.input_files + 'comstock_map.csv', index_col=0) # load here so we dont do it once for every region


def calculate_dsds(region: str):

    # Create figure and axes
    fig, axs = pp.subplots(len(config.end_use_demands.index), 1, figsize=(15, 10)) # 5 rows, 2 columns
    fig.tight_layout()
    fig.subplots_adjust(wspace=0.2, hspace=0.3, top=0.9, left=0.05, right=0.95, bottom=0.05)
    fig.suptitle(f"{region} demand specific distributions (blue).\nWeekly variation overlaid (red).")
    p = 0 # plot tracker

    df_dsd = get_comstock_consumption(region)

    for end_use, eu_config in config.end_use_demands.iterrows():
        
        cols = [col for col in df_dsd.columns if end_use in str(col)]

        for col in cols:

            # Apply weather mapping
            if eu_config['use_weather_map']:

                # Map to weather then normalise
                dsd = weather_mapping.map_data(region, df_dsd[col].to_numpy())
                dsd = np.clip(dsd, 0, np.inf) # north dakota has negative energy hours...
                dsd = dsd / dsd.sum()
                df_dsd[col] = dsd.values
            
            # No weather mapping
            else:

                # Normalise DSD
                df_dsd[col] = df_dsd[col] / df_dsd[col].sum()
        
        # End use aggregate
        df_dsd[end_use] = df_dsd[cols].sum(axis='columns') # sum relevant columns
        df_dsd[end_use] = df_dsd[end_use] / df_dsd[end_use].sum() # normalise

        tow = weather_mapping.get_weekly_variation(df_dsd[end_use].to_numpy()) # time-of-week variation

        # Plot
        axs[p].set_title(end_use)
        axs[p].plot(range(len(df_dsd[end_use])), df_dsd[end_use])
        #axs[p].twinx().plot(range(0,8736,52), tow, 'r-') # time-of-week variation overlaid
        p+=1

    return df_dsd



def get_comstock_consumption(region: str) -> pd.DataFrame:

    buildings = config.params['comstock']['building_types']

    df_comstock = get_comstock_table(region, buildings[0])
    for building in buildings[1:]:
        df = get_comstock_table(region, building)

        # Not all comstock buildings have the same columns
        for col in df.columns:
            if col in df_comstock.columns: df_comstock[col] = df_comstock[col].values + df[col].values
            else: df_comstock[col] = df[col].values

    # Map Comstock columns to end uses, summing if there are multiple mapped to any end use
    for com_col, euf in comstock_map.iterrows():
        euf_col = f"{euf['end_use']} {euf['fuel']}"
        if euf_col in df_comstock.columns: df_comstock[euf_col] += df_comstock[com_col]
        else: df_comstock[euf_col] = df_comstock[com_col]

    eufs = (comstock_map['end_use']+' '+comstock_map['fuel']).values
    df_comstock.drop([col for col in df_comstock.columns if col not in eufs], axis='columns', inplace=True)

    return df_comstock



def get_comstock_table(region: str, building: str) -> pd.DataFrame:

    state = config.regions.loc[region, 'us_state']

    url = config.params['comstock']['url'].replace('<su>', state.upper()).replace('<sl>', state.lower()).replace('<b>', building)
    file = url.split('/')[-1]

    # If already cached, grab and return that
    if os.path.isfile(config.cache_dir + file):
        df = pd.read_csv(config.cache_dir + file, index_col='timestamp')
        print(f"Got {file} from local cache.")
        return df
    
    # Otherwise download from Comstock
    print(f"Downloading {file}...")
    try:
        df = pd.read_csv(url, index_col='timestamp')
    except Exception as e:
        print(f"Failed to download comstock table from url\n{url}")
        raise e

    # Handle timezone change and rearrange so hour 0 is 2018-01-01 00:00
    df = df.iloc[np.arange(-1, len(df)-1)] # starts at 01:00 and ends on 00:00 so roll to 00:00 start
    df = utils.realign_timezone(df, from_timezone='EST') # Comstock comes in EST
    df = df.loc[df.index.minute == 0]

    # Cache locally
    df.to_csv(config.cache_dir + file)
    print(f"Cached {file} locally.")

    return df