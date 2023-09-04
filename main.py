

from util import *
from flows import fields, times, aievent, excel, correspondance



data = get_df_from_parquet_batch(
    size=int(input("size: ") or '0') or None, 
    folder=input("folder: ") or "event",
    offset=(input("offset: ")) or 0
)


for process in [fields, times, aievent, excel, correspondance]:
    if bool(input(f"Run {process.__name__} y|n: ").lower() == "y"):
        process.run(data)