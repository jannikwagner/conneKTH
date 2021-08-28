import pandas as pd

from data_processing import ACTIVITIES

path = "connekth.csv"

with open(path) as file:
    
    df = pd.DataFrame(columns=["name", "email", "zipcode"]+ACTIVITIES)
    lines = file.readlines()

    for line in lines:
        name, email, zipcode, *activities = line.split(",")
        act2 = [activity in activities for activity in ACTIVITIES]
        #print(activities)
        df.loc[len(df)] = [name,email,zipcode]+act2

df.to_csv("users.csv")