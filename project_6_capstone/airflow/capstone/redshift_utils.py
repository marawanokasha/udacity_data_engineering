
class DataQualityChecks:
    
    QUERIES = [
        
        # Size Checks
        ("SELECT COUNT(*) FROM country", lambda x: x[0][0] > 0),
        ("SELECT COUNT(*) FROM state", lambda x: x[0][0] > 0),
        ("SELECT COUNT(*) FROM visa_type", lambda x: x[0][0] > 0),
        ("SELECT COUNT(*) FROM gdp", lambda x: x[0][0] > 0),
        ("SELECT COUNT(*) FROM immigration", lambda x: x[0][0] > 0),
        
        # Validity Checks
        ("SELECT MIN(gdp_value) FROM gdp", lambda x: x[0][0] >= 0), # GDP can't be negative
        ("SELECT MIN(num_previous_stays) FROM immigration", lambda x: x[0][0] >= 0),
        ("SELECT COUNT(*) FROM immigration WHERE arrival_date is NULL", lambda x: x[0][0] == 0),
        ("SELECT DISTINCT day FROM immigration", lambda x: len(set([y[0] for y in x]).difference(set(range(1,32)))) == 0 ),
        ("SELECT DISTINCT month FROM immigration", lambda x: len(set([y[0] for y in x]).difference(set(range(1,13)))) == 0 ),
        ("SELECT MAX(year) FROM immigration", lambda x: x[0][0] < 2030 and x[0][0] > 1900),
        
    ]