import pandas as pd
import numpy as np
import argparse
from datetime import datetime
from pathlib import Path
from rates import ZCBBuilder  # Custom class for zero-coupon rate calculations


EPL_CONFIG_DICT = {  # IN PROD, can go into a YAML file
        "inflow": [
            "GAAP_PREM_INC_COHORT"
        ],
        "outlflow":[
            "GAAP_ANNUITY_OUTGO_COHORT",
            "GAAP_DEATH_OUTGO_COHORT",
            "GAAP_HEALTH_OUTGO_COHORT",
            "GAAP_SURR_OUTGO_COHORT",
            "GAAP_MAT_OUTGO_COHORT"
        ]
    }

RATE_CONFIG_DICT = {
    "ECONOMY": ["USD"],
    "SCENARIO": [8]
}

PROJ_CONFIG_DICT = {
    "PROJ_Y": 5
}


def read_epl_data(val_dt_str, ip_dir: Path) -> pd.DataFrame:
    """
    Reads EPL data from CSV/ fac files, join them together and returns a DataFrame.
    """
    inflows = set(EPL_CONFIG_DICT["inflow"])
    outflows = set(EPL_CONFIG_DICT["outlflow"])
    sign_map = pd.Series(
        data = [1]*len(inflows) + [-1]*len(outflows),
        index = list(inflows) + list(outflows)  
    )

    val_dt = datetime.strptime(val_dt_str, "%Y%m%d").date()  # Convert string to date object
    transformed_list = []

    try:
        fac_files = list(ip_dir.glob("*.fac"))
    except FileNotFoundError:
        raise FileNotFoundError(f"EPL data file not found in {ip_dir}. Please check the directory and file names.")
    
    for file in fac_files:  # TEMPORARY - the data should be available in DATABASE (Super Frodo)
        df = pd.read_csv(file, skiprows=1).iloc[:, 1:]  # Skip the first row and remove the first column
        df = df[df['VAR_NAME'].isin(sign_map.index)]  # Filter rows based on sign_map index
        df.set_index('VAR_NAME', inplace=True)  # Set 'VAR_NAME' as the index
        df = df.mul(sign_map, axis=0)  # Apply the sign_map to the DataFrame
        df = df.T  # Transpose the DataFrame
        df.index = pd.to_datetime(df.index.astype(str), format="%Y%m") + pd.offsets.MonthEnd(0)  # Convert index to datetime and set to month-end
        df.index.name = "YYYYMM"
        df['DEAL_NAME'] = file.stem  # Add a column with the source file name
        transformed_list.append(df)
    
    df_final = pd.concat(transformed_list, axis=0)
    df_final["MONTHS"] = (
        (df_final.index.year - val_dt.year) * 12 +
        (df_final.index.month - val_dt.month)
    )

    df_final = df_final[df_final["MONTHS"] > 0]
    df_final['NET_CF'] = df_final[list(inflows) + list(outflows)].sum(axis=1).round(2).astype(float)  # Calculate net cash flow
    return df_final


def read_scen(val_date, ip_file: Path) -> pd.DataFrame:
    """
    Reads scenario data from CSV file, filters in only necessary set, and returns a DataFrame.
    """
    df = pd.read_csv(ip_file, skiprows=1).iloc[:, 1:]  # Skip the first row and remove the first column
    filtered_df = df[df['ECONOMY'].isin(RATE_CONFIG_DICT["ECONOMY"]) & 
                     df['SCENARIO'].isin(RATE_CONFIG_DICT["SCENARIO"]) &
                     df['CLASS'].isin(['TRE', 'A'])]  # Filter rows based on ECONOMY, SCENARIO, and CLASS
    date_cols = [col for col in filtered_df.columns if col.isdigit() and int(col) < int(val_date[:6])]
    filtered_df = filtered_df.drop(columns=date_cols)  # Drop columns that are not needed
    return filtered_df


def main(debug=False):
    parser = argparse.ArgumentParser(description="Pass valuation date.")
    parser.add_argument("--val_date", type=str, required=True, help="Valudation date in YYYY-MM-DD format")
    args = parser.parse_args()

    try:
        val_date_str = datetime.strptime(args.val_date, "%Y-%m-%d").date().strftime("%Y%m%d")
    except ValueError:
        raise ValueError("Invalid date format. Please use YYYY-MM-DD.")

    base_dir = Path(__file__).resolve().parent    
    epl_df = read_epl_data(val_date_str, base_dir/'EPL'/val_date_str)  # Load the data (EPL data)
    rate_df = read_scen(val_date_str, base_dir/'Rates'/'20241231'/'SCENARIO.fac')  # Load the scenario file (par rates BEY format)
    out_df = pd.DataFrame(columns=["DEAL_NAME", "MONTHS", "PV"])  # Initialize output DataFrame

    for t in range(12*PROJ_CONFIG_DICT["PROJ_Y"] + 1):
        for deal in epl_df['DEAL_NAME'].unique():
            deal_df = epl_df[epl_df['DEAL_NAME'] == deal][['DEAL_NAME', 'MONTHS', 'NET_CF']]  # need currency and simulation as well
            deal_df['MONTHS'] = deal_df['MONTHS'] - t  # to show that this will work with multiple projections
            deal_df = deal_df[deal_df["MONTHS"] > 0]

            # USD and SCEN 
            tenor_mths = rate_df['OS_TERM'].unique()
            yyyymm = (pd.to_datetime(val_date_str, format="%Y%m%d") + pd.offsets.MonthEnd(t)).strftime("%Y%m")
            par_yield_bey = (
                rate_df.loc[rate_df['CLASS'] == 'TRE', yyyymm].reset_index(drop=True) + 
                rate_df.loc[rate_df['CLASS'] == 'A', yyyymm].reset_index(drop=True)
            ).tolist()

            builder = ZCBBuilder(tenor_mths, par_yield_bey)
            zcb_df = builder.run()
            out_df.loc[len(out_df)] = {
                "DEAL_NAME": deal,
                "MONTHS": yyyymm,
                "PV": (deal_df['NET_CF'] * deal_df['MONTHS'].map(zcb_df.set_index('MTHS')['FLOOR_ZCB'])).sum()
            }

    out_df.to_csv('out_mvl.csv', index=False)  # Save the output DataFrame to CSV

    if debug := True:  # Debug mode
        epl_df.to_csv('out_epl.csv')
        rate_df.to_csv('out_rates.csv', index=False)  # Save the rates DataFrame to CSV
        

if __name__ == "__main__":
    main()