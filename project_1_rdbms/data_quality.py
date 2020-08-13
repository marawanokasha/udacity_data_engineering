import logging
import pandas as pd

logger = logging.getLogger(__name__)


# predefined valid user levels which the system should not accept any values outside of them
VALID_USER_LEVELS = {'free', 'paid'}


def check_log_data_quality(filepath, log_data_df: pd.DataFrame):
    """
    Check that log data that we are about to insert into the Database is correct
    """
    
    try:
        assert log_data_df['userId'].isna().sum() == 0
    except:
        logger.error(f'Log file {filepath} contains {log_data_df["userId"].isna().sum()} records with no user id')
        raise

    try:
        invalid_user_levels = set(log_data_df['level'].unique().tolist()).difference(VALID_USER_LEVELS)
        assert len(invalid_user_levels) == 0
    except:
        logger.error(f'Log file {filepath} contains records with the invalid user level(s): {invalid_user_levels}')
        raise
