
import pdb

import pandas as pd

def get_fields(data: pd.DataFrame):
    result = dict()

    for record in data["component_attributes"].to_list():
        if record:
            for field, value in record:
                if field in result:
                    result[field].append(value)
                else:
                    result[field] = [value]
        
    return result

def get_fields_with_sample(data: pd.DataFrame):
    results = get_fields(data)

    result = {}

    for field in results:
        i, found = 0, False
        chosen = None
        while i < len(results[field]) and not found:
            if results[field][i]:
                chosen = results[field][i]
                found = True
            
            i += 1

        result[field] = chosen

    return result


def run(data: pd.DataFrame):
    results = get_fields_with_sample(data)

    pdb.set_trace()