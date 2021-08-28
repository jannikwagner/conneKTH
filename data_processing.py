import pandas as pd
import numpy as np
import scipy as sc

from typing import Sequence

ACTIVITIES = ["Hiking","Running","Climbing","Biking","Volleyball","Football","Basketball","Ice-Hockey","Swimming","Kayaking","Sailing","Surfing","Skiing","Gym","Workout","Gymnastics","Acrobatics","Aerobic","Tennis","Yoga","Pilates","Medidation","Nutrition","Gender equality","LGBTQ","Sustainability","Walking","Fika","Travel","Sightseeing","Board Games","Darts","Billiards","Partying","Festivals and Concerts","Fashion and Beauty","New language","New culture","Swedish culture","New skillset","Cooking","Foodies","Bars","Sci-Fi","Gaming","Coding","3D Printing","Writing","Reading","Anime","Drawing","Painting & other","Crafts","Photography","Film and movies","Theatre","Dance","Playing Instruments","Singing/ Choir","Listening to music","Dj-ing"]
PHYSICAL_DISTANCE_THRESHOLD = 10
SCORE_THRESHOLD = 5
MIN_GROUP_SIZE = 5
MAX_GROUP_SIZE = 10
NUM_GROUPS_PER_USER = 3
GROUPS_PATH = "groups.csv"
USERS_PATH = "users.csv"
FULL_GROUPS_PATH = "full_groups.csv"


class User:
    def __init__(self, row: pd.DataFrame):
        self.df = row
    
    def get_activities(self):
        return self.df[ACTIVITIES]

    def get_email(self):
        return self.df["email"]
    
    def get_activity(self, activity: str):
        return self.df[activity]


class Group:
    def __init__(self, users: pd.DataFrame, ID: int, topic: str):
        self.df = users
        self.ID = ID
        self.topic = topic
    
    def get_shared_activities(self) -> pd.DataFrame:
        return self.df[ACTIVITIES].any()

    def get_average_activities(self):
        return self.df[ACTIVITIES].mean()

    def get_size(self):
        return len(self.df)

    def add_user(self, user: User):
        self.df = self.df.append(user.df)
        if self.get_size() == MIN_GROUP_SIZE:
            self.topic = self.df.columns[self.df.sum().argmax()]


class Users:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_user_by_email(self, email: str) -> User:
        return User(self.df.query("email == @email"))

    def get_user_by_id(self, ID: int) -> User:
        return User(self.df.iloc[ID])

    def get_group_by_emails(self, emails: Sequence[str], ID, topic):
        return Group(self.df.query("email in @emails"), ID, topic)


class StreamClustering:
    def __init__(self, users_path: str, groups_path: str):
        self.users = self.read_users(users_path)
        self.groups = self.read_groups(groups_path)
    
    def read_users(self, path: str):
        df = self.read_csv(path)
        return Users(df)

    def read_csv(self, path):
        df = pd.read_csv(path)
        df.index = df["Unnamed: 0"].values
        df = df.drop("Unnamed: 0", axis=1)
        return df

    def read_groups(self, path: str):
        try:
            df = self.read_csv(path)
        except:
            df = pd.DataFrame()
        return [self.users.get_group_by_emails(df.loc[index][1:], index, df.loc[index][0]) for index in df.index]
    
    def metric(self, x, y):
        return np.sqrt(np.sum(x*y))

    def get_score(self, user: User, group: Group):
        if group.get_size() < MIN_GROUP_SIZE:
            if not np.any(np.logical_and(user.get_activities(), group.get_shared_activities())):
                return np.inf
        else:
            if not user.get_activity(group.topic):
                return np.inf
        return self.metric(user.get_activities(), group.get_average_activities())

    def add_user(self, user: User):
        scores = [(group, self.get_score(user, group)) for group in self.groups]
        scores = [(group, score) for group, score in scores if score < SCORE_THRESHOLD]
        if len(scores) > 0:
            p = np.array([1/score for group, score in scores])
            p = p / np.sum(p)
            chosable_groups = np.array([group for group, score in scores])
            chosen_groups = np.random.choice(chosable_groups, min(NUM_GROUPS_PER_USER, len(chosable_groups)), p=p)
            for group in chosen_groups:
                group.add_user(user)

        else:
            ID = 0 if len(self.groups) == 0 else max([group.ID for group in self.groups])+1
            group = self.users.get_group_by_emails([user.get_email()], ID, None)
            self.groups.append(group)

    def save_groups(self, groups_path: str, full_groups_path: str):
        not_full_df = pd.DataFrame(data=None, columns=["topic", *range(MAX_GROUP_SIZE)])
        try:
            full_df = pd.read_csv(full_groups_path)
        except:
            full_df = pd.DataFrame(data=None, columns=["topic", *range(MAX_GROUP_SIZE)])
        for group in self.groups:
            if group.get_size() < MAX_GROUP_SIZE:
                row = [None] * (1+MAX_GROUP_SIZE)
                row[0:1+len(group.df["email"].values)] = [group.topic, *group.df["email"].values]
                not_full_df.loc[group.ID] = row
            else: 
                full_df = full_df.append([group.ID, group.topic, *group.df.values])
        not_full_df.to_csv(groups_path)
        full_df.to_csv(full_groups_path)


if __name__ == "__main__":
    sc = StreamClustering(users_path=USERS_PATH, groups_path=GROUPS_PATH)
    sc.add_user(sc.users.get_user_by_id(5))
    sc.save_groups(GROUPS_PATH, FULL_GROUPS_PATH)
