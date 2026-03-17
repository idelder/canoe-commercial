"""
Aggregates new capacity data
Written by Ian David Elder for the CANOE model
"""

from setup import config
import sqlite3
import utils
import pandas as pd
from currency_conversion import conv_curr



def aggregate_region(region: str, df_exs: pd.DataFrame):

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    aeo_year = config.params['aeo_installed_year']
    comstock_year = config.params['comstock']['data_year']


    """
    ##############################################################
        New technologies
    ##############################################################
    """

    for tech, tech_config in config.new_techs.iterrows():

        end_use = tech_config['end_use']

        # NOT CURRENTLY IN USE - omitting minACF instead
        # Annual capacity factor cannot be higher than the area under the DSD curve
        # otherwise the peak output of a process must be higher than its capacity allows (impossible)
        # since all end use outputs follow the same normalised curve as the DSD
        #dsd = df_dsd[end_use]
        #acf_lim = dsd.mean() / dsd.max() * (1 - config.params['acf_buffer'])

        if end_use != 'space heating' and end_use != 'space cooling': continue
        if (end_use, tech_config['fuel']) not in df_exs.index: continue # insufficient data for this technology
        
        # Prepare some stuff
        fuel_config = config.fuel_commodities.loc[tech_config['fuel']]
        eu_config = config.end_use_demands.loc[end_use]

        # AEO data
        aeo_data = config.aeo_cdm.loc[config.aeo_cdm['techname'] == tech_config['aeo_tech']]
        if type(aeo_data) is pd.DataFrame:
            try: aeo_data = aeo_data.iloc[0] # if multiple rows, just take first
            except Exception as e:
                print(f"Failed. Could not find AEO data for {tech_config['aeo_tech']}.")
                raise e


        ## Technologies
        curs.execute(
            f"""REPLACE INTO
            Technology(tech, flag, sector, annual, description, data_id)
            VALUES('{tech}', 'p', 'commercial', 1, '{end_use} {tech_config['description']}', '{utils.data_id()}')"""
        )


        ## LifetimeTech
        life = round(aeo_data['life'])
        note = f"Rounded life from AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
        ref = config.refs.get('aeo')
        curs.execute(
            f"""REPLACE INTO
            LifetimeTech(region, tech, lifetime,
            notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id) 
            VALUES('{region}', '{tech}', {life},
            '{note}', '{ref.id}', 1, 2, 1, 2, 3, '{utils.data_id(region)}')"""
        )
        

        ## CapacityToActivity
        c2a = 1 # Capacity is in PJ/y and activity is in PJ
        note = "Capacity is in PJ/y and activity is in PJ so 1"
        curs.execute(
            f"""REPLACE INTO
            CapacityToActivity(region, tech, c2a, notes, data_id)
            VALUES('{region}', '{tech}', {c2a}, '{note}', '{utils.data_id(region)}')"""
        )


        # Only indexed by vintage
        for vint in config.model_periods:

            ## Efficiency
            eff = aeo_data['efficiency']
            note = f"From AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
            ref = config.refs.get('aeo')
            curs.execute(
                f"""REPLACE INTO
                Efficiency(region, input_comm, tech, vintage, output_comm, efficiency,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{region}', '{fuel_config['comm']}', '{tech}', {vint}, '{eu_config['comm']}', {eff},
                '{note}', '{ref.id}', 1, 2, 3, 2, 2, '{utils.data_id(region)}')"""
            )
            

            ## CostInvest
            cost_invest = aeo_data['capcst'] * config.params['conversion_factors']['cost']['aeo']
            cost_invest = conv_curr(cost_invest)
            note = f"Capcst from AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
            ref = config.refs.get('aeo')
            curs.execute(
                f"""REPLACE INTO
                CostInvest(region, tech, vintage, cost, units,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{region}', '{tech}', {vint}, {cost_invest}, 'M$/PJ/y',
                '{note}', '{ref.id}', 1, 2, 1, 2, 2, '{utils.data_id(region)}')"""
            )
            

            # Indexed by period and vintage
            for period in config.model_periods:

                if vint > period or vint + life <= period: continue

                ## CostFixed
                cost_fixed = aeo_data['maintcst'] * config.params['conversion_factors']['cost']['aeo']
                cost_fixed = conv_curr(cost_fixed)
                note = f"Maintcst from AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
                ref = config.refs.get('aeo')
                curs.execute(
                    f"""REPLACE INTO
                    CostFixed(region, period, tech, vintage, cost, units,
                    notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                    VALUES('{region}', {period}, '{tech}', {vint}, {cost_fixed}, 'M$/PJ',
                    '{note}', '{ref.id}', 1, 2, 1, 2, 2, '{utils.data_id(region)}')"""
                )


            ## AnnualCapacityFactor
            acf = df_exs.loc[(end_use, tech_config['fuel']), 'acf']
            note = f"Mean hourly demand divided by peak hourly demand from Comstock (NREL, {comstock_year})"
            ref = config.refs.get('comstock')
                    
            curs.execute(
                f"""REPLACE INTO
                LimitAnnualCapacityFactor(region, tech, vintage, output_comm, operator, factor,
                notes, data_source, dq_cred, dq_geog, dq_struc, dq_tech, dq_time, data_id)
                VALUES('{region}', '{tech}', {vint}, '{eu_config['comm']}', 'le', {acf},
                '{note}', '{ref.id}', 1, 2, 5, 2, 3, '{utils.data_id(region)}')"""
            )
            
    
    conn.commit()
    conn.close()