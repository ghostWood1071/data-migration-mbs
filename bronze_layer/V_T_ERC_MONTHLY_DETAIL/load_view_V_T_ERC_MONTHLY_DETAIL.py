from common_functions import load_data_from_oracledb_to_bronze

load_data_from_oracledb_to_bronze("MISUSER", "V_T_ERC_MONTHLY_DETAIL", ["C_MONTH","C_YEAR"])