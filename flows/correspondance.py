
import pandas as pd
from flows import aievent, excel, times
import util

import pdb


def run(data: pd.DataFrame):

    print("Retrieving event data...")

    aievent_data = util.get_df_from_parquet_batch(
        folder="event"
    )

    attrs = aievent.get_correspondance_guids(aievent_data)

    data = excel.get_relevant_data(data)

    data_guid_field = input("Data GUID field: ") or "AAcDbDwgVersionGuid"

    src_used_guids = data[data[data_guid_field].isin(attrs["AAISrcVersionGuid"])]
    dst_used_guids = data[data[data_guid_field].isin(attrs["AAIDstVersionGuid"])]

    times.run(src_used_guids)
    times.run(dst_used_guids)


    pdb.set_trace()

