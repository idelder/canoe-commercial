"""
Aggregates residential non-subsector-specific data
Written by Ian David Elder for the CANOE model
"""

from setup import config
import utils
import pandas as pd
import sqlite3
import os
import comstock_dsd
import existing_capacity
import new_capacity

# Shortens lines a bit
fuel_commodities = config.fuel_commodities
end_use_demands = config.end_use_demands
conversion_factors = config.params['conversion_factors']



def aggregate():

    pre_process()

    # Aggregate space heating and cooling
    for region in config.model_regions:
        
        print(f"Aggregating {region}...\n")

        df_dsd = comstock_dsd.calculate_dsds(region)
        df_exs = existing_capacity.aggregate_region(region, df_dsd)
        new_capacity.aggregate_region(region, df_exs, df_dsd) # existing data for annual capacity factors
        
        print(f"Aggregated {region}.\n")

    if config.params['include_emissions']: aggregate_emissions()
    if config.params['include_imports']: aggregate_imports()

    post_process()

    



# For non-regional aggregation
def pre_process():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db


    """
    ##############################################################
        Basic parameters
    ##############################################################
    """

    for h, row in config.time.iterrows():
        curs.execute(f"""REPLACE INTO
                     time_season(t_season)
                     VALUES('{row['season']}')""")
        curs.execute(f"""REPLACE INTO
                     time_of_day(t_day)
                     VALUES('{row['time_of_day']}')""")
        curs.execute(f"""REPLACE INTO
                     SegFrac(season_name, time_of_day_name, segfrac)
                     VALUES('{row['season']}', '{row['time_of_day']}', {1/8760})""")
        
    for period in [*config.model_periods, config.model_periods[-1] + config.params['period_step']]:
        curs.execute(f"""REPLACE INTO
                     time_periods(t_periods, flag)
                     VALUES({period}, 'f')""")
        
    for label, description in {'f': 'future', 'e': 'existing'}.items():
        curs.execute(f"""INSERT OR IGNORE INTO
                     time_period_labels(t_period_labels, t_period_labels_desc)
                     VALUES('{label}', '{description}')""")

    for region, row in config.regions.iterrows():
        if row['include']:
            curs.execute(f"""REPLACE INTO
                        regions(regions, region_note)
                        VALUES('{region}', '{row['description']}')""")
    
    curs.execute(f"DELETE FROM GlobalDiscountRate") # has no indexing
    curs.execute(f"""REPLACE INTO
                GlobalDiscountRate(rate)
                VALUES({config.params['global_discount_rate']})""")


    """
    ##############################################################
        Commodities
    ##############################################################
    """
    
    for _code, comm_config in config.fuel_commodities.iterrows():
        curs.execute(f"""REPLACE INTO
                    commodities(comm_name, flag, comm_desc)
                    VALUES('{comm_config['comm']}', 'p', '({comm_config['unit']}) {comm_config['description']}')""")
        
    # CO2-equivalent emission commodity
    curs.execute(f"""REPLACE INTO
                commodities(comm_name, flag, comm_desc)
                VALUES('{config.params['emission_commodity']}', 'e', '(ktCO2eq) CO2-equivalent emissions')""")


    conn.commit()
    conn.close()

    print(f"Pre-aggregation complete.\n")
        


# For non-regional post-subsector aggregation
def post_process():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db
    

    """
    ##############################################################
        Existing time periods
    ##############################################################
    """

    # Add all existing vintages to existing time periods
    vints = set([fetch[0] for fetch in curs.execute(f"SELECT vintage FROM Efficiency").fetchall() if fetch[0] not in config.model_periods])

    for vint in vints:
        curs.execute(f"""INSERT OR IGNORE INTO
                        time_periods(t_periods, flag)
                        VALUES({vint}, 'e')""")


    conn.commit()
    conn.close()

    print(f"Post-aggregation complete.\n")



def aggregate_emissions():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db
    

    """
    ##############################################################
        Emission Activity
    ##############################################################
    """

    emis_comm = config.params['emission_commodity']
    emis_units = config.params['emission_activity_units']

    # Get emissions factors for fuels in ktCO2eq/PJ_in
    emis_fact = utils.get_data('https://www.epa.gov/system/files/documents/2024-02/ghg-emission-factors-hub-2024.xlsx', skiprows=14, nrows=76, index_col=2)
    emis_fact = emis_fact[['CO2 Factor', 'CH4 Factor', 'N2O Factor']].iloc[1::].dropna()
    emis_fact = emis_fact[pd.to_numeric(emis_fact['CO2 Factor'], errors='coerce').notnull()] # Removing NaN rows
    for fact in emis_fact.columns: emis_fact[fact] = emis_fact[fact].astype(float) * conversion_factors['epa_units'][fact.strip(' Factor')] * conversion_factors['gwp'][fact.strip(' Factor')]
    emis_fact[emis_comm] = emis_fact.sum(axis=1)

    for tech in config.all_techs:

        # Valid vintages and efficiencies from Efficiency table
        rows = curs.execute(f"SELECT regions, input_comm, tech, vintage, output_comm, efficiency FROM Efficiency WHERE tech == '{tech}'").fetchall()

        for row in rows:

            # Input fuel by epa naming convention
            epa_fuel = fuel_commodities.loc[fuel_commodities['comm'] == row[1], 'epa_fuel'].iloc[0]
            if pd.isna(epa_fuel): continue # doesn't need emissions

            # EmissionActivity is tied to OUTPUT energy so divide by efficiency
            emis_act = emis_fact.loc[epa_fuel, emis_comm] / row[5]

            # Note assumed fuel
            note = f"Emissions factor using {epa_fuel} (EPA, {config.params['epa_year']}) divided by efficiency as emissions are per output unit energy."

            curs.execute(f"""REPLACE INTO
                        EmissionActivity(regions, emis_comm, input_comm, tech, vintage, output_comm, emis_act, emis_act_units, emis_act_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{row[0]}', '{emis_comm}', '{row[1]}', '{row[2]}', {row[3]}, '{row[4]}', {emis_act}, '{emis_units}', '{note}',
                        '{config.params['epa_reference']}', {config.params['epa_year']}, 2, 1, 1, 1, 1, 3)""")
    

    conn.commit()
    conn.close()

    print(f"Emissions data aggregated into {os.path.basename(config.database_file)}\n")



def aggregate_imports():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    # Get which fuel commodities are actually being used
    df_eff = pd.read_sql_query("SELECT * FROM Efficiency", conn)

    for tech, row in config.import_techs.iterrows():
        
        # Get CANOE nomenclature for imported commodity
        out_comm = config.fuel_commodities.loc[row['out_comm']]

        # Make sure the model is using this imported commodity otherwise skip
        if out_comm['comm'] not in df_eff['input_comm'].values:
            print(out_comm['comm'])
            continue
        
        description = f"import dummy for {out_comm['description']}"

        curs.execute(f"""REPLACE INTO
                     technologies(tech, flag, sector, tech_desc)
                     VALUES('{tech}', 'r', 'commercial', '{description}')""")
        
        # A single vintage at first model period with no other parameters, classic dummy tech
        for region in config.model_regions:

            # Make sure the model is using this imported commodity in this region otherwise skip
            if df_eff[(df_eff['regions'] == region) & (df_eff['input_comm'] == out_comm['comm'])].empty:
                print(f"Import {tech} skipped for region {region} as the fuel isn't used.")
                continue

            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes)
                        VALUES('{region}', '{config.fuel_commodities.loc[row['in_comm'], 'comm']}', '{tech}',
                        '{config.model_periods[0]}', '{out_comm['comm']}', 1, '{description})')""")
            
    conn.commit()
    conn.close()

    print(f"Imports aggregated into {os.path.basename(config.database_file)}\n")



if __name__ == "__main__":
    
    aggregate()