import logging
from pyspark.sql.functions import count, col

def check_empty_df(df) -> None:
    '''
    If dataframe is empty, raise an error.
    df = Dataframe
    returns: None
    '''
    if df.rdd.isEmpty():
        logging.warning("Empty Dataframe.")
        return False

    return True

def check_missing_columns(columns_expected: list, columns: list) -> bool:
    '''
    If a column from columns_expected is not present in the columns list, raise an error.
    columns_excpeted: List of expected columns
    columns: List of columns in the dataframe
    returns: None
    '''
    missing_columns = [col for col in columns_expected  if col not in columns]

    if missing_columns:
        logging.warning(f"Missing columns for expected schema: {missing_columns}")
        return False

    return True


def check_only_unique_rows(df, columns: list) -> bool:
    '''
    Check if a Dataframe has duplicated values for the columns received.
    df: Dataframe
    columns: List of columns used for selecting unique values
    return: Boolean
    '''

    df_duplicate_rows = df.groupBy(columns).agg(count('*').alias('quantity')).filter(col('quantity') > 1)
    if check_empty_df(df_duplicate_rows):
        logging.error(f"Duplicated rows for columns {columns}")
        return False
    else:
        logging.info(f"No duplicated rows for columns {columns} found")
        return True