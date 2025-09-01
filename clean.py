import os 
import re
import pandas as pd

df = pd.read_csv("combined.csv",dtype=str)

df = df.reset_index()
df = df.astype(str) 


# Regex templates
templates = {
    "jobId": re.compile(r"^\d{2}-\d{6}$"),
    "date": re.compile(r"^\d{2}/\d{2}/\d{4}$"),
    "location": re.compile(r"^[A-Za-z ]+, [A-Z]{2}$")
}

def realign_df_inplace(df, templates):
    # make sure our three columns are strings
    df[["jobId", "date", "location"]] = df[["jobId", "date", "location"]].astype(str)
    
    for idx, row in df.iterrows():
        values = [row["jobId"], row["date"], row["location"]]
        fixed = {}
        unused = values[:]
        
        # assign based on regex match
        for col, pattern in templates.items():
            for val in unused:
                if pattern.match(str(val)):
                    fixed[col] = val
                    unused.remove(val)
                    break
        
        # if something didn't match, keep it in leftover order
        for col in ["jobId", "date", "location"]:
            if col not in fixed and unused:
                fixed[col] = unused.pop(0)
        
        # update the DataFrame row safely
        for col in ["jobId", "date", "location"]:
            df.at[idx, col] = fixed[col]


realign_df_inplace(df, templates)

print(df)

    
df.to_csv("cleaned.csv", index=False)

