import pandas as pd
import numpy as np
import scipy as sc

from typing import Sequence

ACTIVITIES = ["Football", "Skiing"]


class User:
    def __int__(self, row: pd.DataFrame):
        self.df = row
    
    def get_activities(self):
        return self.df[ACTIVITIES]


class Group:
    def __init__(self, users: pd.DataFrame):
        self.df = users
    
    def get_shared_activities(self) -> pd.DataFrame:
        return self.df[ACTIVITIES]


class Users:
    def __init__(self, path: str):
        self.df = pd.read_csv(path)

    def get_user_by_email(self, email: str) -> User:
        return User(self.df.query("email == @email"))

    def get_user_by_id(self, id: int) -> User:
        return User(df.iloc[id])

    def get_groups_by_emails(self, emails: Sequence[str]):
        return Group(self.df.query("email in @emails"))


def get_score(user: User, group: Group):
    pass

        


def add_user(user, groups):
    dissimilarities = {group: get_score(user, group) for group in groups}
