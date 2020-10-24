import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DateType, BooleanType


@udf(DateType())
def get_date_from_i94_date_int(x):
    if x is None:
        return x
    if x < 0:
        return datetime.datetime(1900, 1, 1)  # default date for invalid dates
    else:
        return datetime.datetime(1960, 1, 1) + datetime.timedelta(x)


@udf(StringType())
def normalize_gender(x):
    if x in {"M", "F"}:
        return x
    elif x is None:
        return "UNSPECIFIED"
    else:
        return "OTHER"


@udf(BooleanType())
def normalize_match_flag(x):
    if x == None:
        return False
    else:
        return True


@udf(StringType())
def normalize_mode(x):
    if x == 1:
        return "Air"
    elif x == 2:
        return "Sea"
    elif x == 3:
        return "Land"
    elif x is None:
        return None
    else:
        return "Other"


@udf(StringType())
def normalize_visa_category(x):
    if x == 1:
        return "Business"
    elif x == 2:
        return "Pleasure"
    elif x == 3:
        return "Student"
    elif x is None:
        return None
    else:
        return "Other"
