"""
Builds commercial buildings sector database
Written by Ian David Elder for the CANOE model
"""

import os
import utils
import all_subsectors
import currency_conversion
import setup
import sqlite3
from setup import config
from matplotlib import pyplot as pp



def build_database():

    print(f"Aggregating commercial sector into {os.path.basename(config.database_file)}...\n")

    setup.instantiate_database()

    all_subsectors.aggregate()

    # Convert data costs to final currency
    currency_conversion.convert_currencies()

    if config.params['clone_to_xlsx']: utils.database_converter().clone_sqlite_to_excel()

    prep_high_res_testing()

    print(f"Commercial sector aggregated into {os.path.basename(config.database_file)}\n")

    # Show any plots that have been made
    if config.params['show_plots']: pp.show()



"""
##############################################################
    The following is temporary for buildings sector testing
##############################################################
"""

def prep_high_res_testing():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()
    
    fuel_costs = {
        "NG": 8.847,
        "OIL": 25.163,
        "ELC": 31.944,
    }

    base_emis = {
        "ON": 16000,
        "AB": 8000,
        "BC": 3000,
        "MB": 1600,
        "SK": 1700,
        "QC": 4200,
        "NS": 1300
    }

    emis = {
        2021: 1.00,
        2025: 0.85,
        2030: 0.7,
        2035: 0.5,
        2040: 0.30,
        2045: 0.15,
        2050: 0.00
    }
                
    rep_days = [
        'D006', # Coldest day ON 2018
        'D035',
        'D070',
        'D105',
        'D140',
        'D186' # Hottest day ON 2018
    ]

    seas_tables = [
        'DemandSpecificDistribution'
    ]

    # Delete all days but rep days above
    curs.execute(f"DELETE FROM time_season")
    [curs.execute(f"INSERT OR IGNORE INTO time_season(t_season) VALUES('{day}')") for day in rep_days]

    for table in seas_tables:
        curs.execute(f"DELETE FROM {table} WHERE season_name NOT IN (SELECT t_season from time_season)")

    curs.execute(f"DELETE FROM SegFrac")
    for day in rep_days:
        for h in range(24):
            curs.execute(f"""REPLACE INTO SegFrac(season_name, time_of_day_name, segfrac)
                        VALUES('{day}', '{config.time.loc[h, 'time_of_day']}', {1/(24*6)})""")
            
    # Renormalise dsd
    for end_use in config.end_use_demands['comm']:
        for region in config.model_regions:
            total_dsd = sum([dsd[0] for dsd in curs.execute(f"""SELECT dsd FROM DemandSpecificDistribution
                                                            WHERE demand_name == '{end_use}' AND regions == '{region}'""").fetchall()])
            curs.execute(f"""UPDATE DemandSpecificDistribution
                        SET dsd = dsd / {total_dsd}
                        WHERE demand_name == '{end_use}' and regions = '{region}'""")

    # Add fuel imports and costs
    for fuel, cost in fuel_costs.items():
            for period in config.model_periods:
                curs.execute(f"""REPLACE INTO
                            CostVariable(regions, periods, tech, vintage, cost_variable, cost_variable_units, data_cost_year, data_curr, data_flags)
                            VALUES('{region}', {period}, 'C_IMP_{fuel}', {config.model_periods[0]}, {cost}, 'TEST VAL M$/PJ', 2020, 'CAD', 'TEST')""")

    for region in config.model_regions:
        for period in config.model_periods:
            curs.execute(f"""REPLACE INTO
                        EmissionLimit(regions, periods, emis_comm, emis_limit, emis_limit_units)
                        VALUES('{region}', {period}, "CO2eq", {emis[period]*base_emis[region]}, "ktCO2eq")""")
        
    conn.commit()
    conn.close()

    print("Finished.")



if __name__ == "__main__":
    
    build_database()