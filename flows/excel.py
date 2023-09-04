
import pdb
import pandas as pd

EXCEL_FILE_NAME = "excel"

EXCLUDED_COLS = {'AAcDbFonts'}

def get_relevant_data(data: pd.DataFrame):
    
    attributes = data["component_attributes"].apply(pd.Series)

    columns_names = {key:value[0][0] for key, value in  attributes[0:1].to_dict().items()}

    attributes = attributes.rename(columns=columns_names)

    for col in attributes.columns:
        if col not in EXCLUDED_COLS:
            attributes[col] = attributes[col].str.get(1)
    
    attributes[["device_time", "device_id"]] = data[["device_time", "device_id"]]

    return attributes



def run(data: pd.DataFrame):

    attrs = get_relevant_data(data)

    attrs.to_excel(f"{input('file name: ') or EXCEL_FILE_NAME}.xlsx", sheet_name="main")

    pdb.set_trace()



    