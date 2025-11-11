from common_functions import load_data_from_oracledb_to_bronze

load_data_from_oracledb_to_bronze("BACK", "T_TLO_DEBIT_BALANCE_HISTORY", "C_TRANSACTION_DATE")