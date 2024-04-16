"""
Aggregates new capacity data
Written by Ian David Elder for the CANOE model
"""

from setup import config
import sqlite3
import utils
import pandas as pd



def aggregate_region(region: str, df_exs: pd.DataFrame, df_dsd: pd.DataFrame):

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()

    aeo_ref = config.params['aeo_reference']
    aeo_year = config.params['aeo_installed_year']
    comstock_year = config.params['comstock']['data_year']
    comstock_ref = config.params['comstock']['reference']


    """
    ##############################################################
        New technologies
    ##############################################################
    """

    for tech, tech_config in config.new_techs.iterrows():

        end_use = tech_config['end_use']

        # Annual capacity factor cannot be higher than the area under the DSD curve
        # otherwise the peak output of a process must be higher than its capacity allows (impossible)
        # since all end use outputs follow the same normalised curve as the DSD
        dsd = df_dsd[end_use]
        acf_lim = dsd.mean() / dsd.max() * (1 - config.params['acf_buffer'])

        if end_use != 'space heating' and end_use != 'space cooling': continue
        if (end_use, tech_config['fuel']) not in df_exs.index: continue # insufficient data for this technology
        
        # Prepare some stuff
        fuel_config = config.fuel_commodities.loc[tech_config['fuel']]
        eu_config = config.end_use_demands.loc[end_use]

        # AEO data
        aeo_data = config.aeo_cdm.loc[config.aeo_cdm['techname'] == tech_config['aeo_tech']]
        if type(aeo_data) is pd.DataFrame: aeo_data = aeo_data.iloc[0] # if multiple rows, just take first


        ## Technologies
        curs.execute(f"""REPLACE INTO
                     technologies(tech, flag, sector, tech_desc)
                     VALUES('{tech}', 'p', 'commercial', '{end_use} {tech_config['description']}')""")


        ## LifetimeTech
        life = round(aeo_data['life'])
        note = f"Rounded life from AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
        reference = aeo_ref
        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {life}, '{note}',
                    '{reference}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
        

        ## CapacityToActivity
        c2a = 1 # Capacity is in PJ/y and activity is in PJ
        note = "Capacity is in PJ/y and activity is in PJ so 1"
        curs.execute(f"""REPLACE INTO
                    CapacityToActivity(regions, tech, c2a, c2a_notes)
                    VALUES('{region}', '{tech}', {c2a}, '{note}')""")


        # Only indexed by vintage
        for vint in config.model_periods:

            ## Efficiency
            eff = aeo_data['efficiency']
            note = f"From AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
            reference = aeo_ref
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{fuel_config['comm']}', '{tech}', {vint}, '{eu_config['comm']}', {eff}, '{note}',
                        '{reference}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
            

            ## CostInvest
            cost_invest = aeo_data['capcst'] * config.params['conversion_factors']['cost']['aeo']
            note = f"Capcst from AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
            reference = aeo_ref
            curs.execute(f"""REPLACE INTO
                        CostInvest(regions, tech, vintage, cost_invest_notes, data_cost_invest, data_cost_year, data_curr,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, '{note}', {cost_invest}, {config.params['aeo_currency_year']}, '{config.params['aeo_currency']}',
                        '{reference}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
            

            # Indexed by period and vintage
            for period in config.model_periods:

                if vint > period or vint + life <= period: continue

                ## CostFixed
                cost_fixed = aeo_data['maintcst'] * config.params['conversion_factors']['cost']['aeo']
                note = f"Maintcst from AEO CDM ktekx technology menu for technology {tech_config['aeo_tech']} (AEO, {aeo_year})"
                reference = aeo_ref
                curs.execute(f"""REPLACE INTO
                            CostFixed(regions, periods, tech, vintage, cost_fixed_notes, data_cost_fixed, data_cost_year, data_curr,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', {vint}, '{note}', {cost_fixed}, {config.params['aeo_currency_year']}, '{config.params['aeo_currency']}',
                            '{reference}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")


        ## AnnualCapacityFactor
        acf = df_exs.loc[(end_use, tech_config['fuel']), 'acf']
        note = f"Mean hourly demand divided by peak hourly demand from Comstock (NREL, {comstock_year})"
        reference = comstock_ref

        if acf > acf_lim:
            acf = acf_lim
            note += f". Bounded to mean(DSD)/max(DSD) for {end_use}"
            print(f"Warning! Annual capacity factor for {region} {tech} {tech_config['end_use']} was too high and had to be bounded. "
                    "ACF cannot be higher than mean(DSD)/max(DSD) or the model will have no solution.")
            
        for period in config.model_periods:
                
            curs.execute(f"""REPLACE INTO
                        MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{tech}', '{eu_config['comm']}', {acf*0.99}, '{note}. Times 0.99 for computational slack.',
                        '{reference}', {comstock_year}, 4, 2, 1, {utils.dq_time(comstock_year, period)}, 3, 3)""")
            curs.execute(f"""REPLACE INTO
                        MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{tech}', '{eu_config['comm']}', {acf}, '{note}',
                        '{reference}', {comstock_year}, 4, 2, 1, {utils.dq_time(comstock_year, period)}, 3, 3)""")
            
    
    conn.commit()
    conn.close()