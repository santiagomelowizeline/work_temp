
import pdb

import pandas as pd


SAVE_OPERATIONS = ["SAVE", "SAVEAS", "UNKNOWN_EDIT"]

def get_correspondance_guids(data: pd.DataFrame):

    attributes = data["component_attributes"].apply(pd.Series)

    attributes = attributes[[3, 4, 11]].rename({3: 'AAISrcVersionGuid', 4: 'AAIDstVersionGuid', 11: 'AAIOperation'}, axis=1)

    for col in attributes.columns:
        attributes[col] = attributes[col].str.get(1)
    
    # Filter out non-save event operations
    attributes = attributes[attributes["AAIOperation"].isin(SAVE_OPERATIONS) ]

    # Join Src GUIDs per Dst GUID
    # attributes = attributes.groupby("AAIDstVersionGuid").agg({'AAISrcVersionGuid': lambda x: x})

    return attributes


def run(data: pd.DataFrame):

    attrs = get_correspondance_guids(data)

    pdb.set_trace()