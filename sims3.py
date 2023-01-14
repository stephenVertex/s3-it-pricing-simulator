#!/usr/bin/env python3
import uuid
import datetime
from enum import Enum
import random
import numpy as numpy
import pandas as pd

class Tier(Enum):
    FA  = 1
    IA  = 2
    AII = 3


class S3Object:

    def __init__(self, 
                 creation_date, 
                 size_gb, 
                 current_tier = Tier.FA, 
                 ):
        self.creation_date = creation_date
        self.last_get_date = None
        self.last_put_date = creation_date
        self.size_gb       = size_gb
        self.current_tier  = current_tier
        self.time_in_tier  = [0]
        self.transitions   = []
        self.id            = uuid.uuid4()
        self.get_count     = 0
        self.put_count     = 1
        self.list_count    = 0

class ObjectGroup:
    def __init__(self, members):
        self.group_id = uuid.uuid4()
        self.members  = members  # Members is a set, ranging from 2-20 items.

class Account:
    def __init__(self, member_set):
        self.object_groups = {} ## Map group_id -> ObjectGroup

    def add_object_group(self, og):
        self.object_groups[og.group_id] = og


class S3Sim:
    def __init__(self, start_date = datetime.date(year=2022, month=1, day=1)):
        self.objects_per_day_added = 100
        self.objects_per_group_min = 3
        self.objects_per_group_max = 20
        self.object_size_gb_min    = 0.001
        self.object_size_gb_max    = 0.3
        self.days_to_simulate      = 90
        self.account = Account(set())
        self.start_date = start_date
        ## The simulation starts at the beginnging of the current day
        self.cur_date = start_date
        self.obj_to_group = {}
        self.date_to_groups = {}
        self.object_list = []

    def simulate_day(self):
        print(f"{self.cur_date}")

        ## Sample how many objects to add, divide these objects into groups
        num_new_objs = 0
        group_ids = set()
        while num_new_objs < self.objects_per_day_added:
            N_group = random.choice(range(self.objects_per_group_min, self.objects_per_group_max))
            #print(f"Sampled: {N_group}")
            num_new_objs = num_new_objs + N_group
            objs = set()
            ## Create the individual objects
            for i in range(N_group):
                size_gb = random.uniform(self.object_size_gb_min, self.object_size_gb_max)
                o = S3Object(self.cur_date, size_gb)
                objs.add(o)
                self.object_list.append(o)
            g = ObjectGroup(objs)

            ## Fill out the secondary maps
            for m in g.members:
                self.obj_to_group[m.id] = g.group_id
            group_ids.add(g.group_id)    

            print(f"Generated group {g.group_id} with {len(g.members)} members")
            self.account.add_object_group(g)
        self.date_to_groups[self.cur_date] = group_ids
        return(None)


    def summarize(self):
        days = list(sort(self.date_to_group.keys()))
        print(f"S3 Account Summarizing {len(days)}")

        total_num_objects = 0
        total_obj_size    = 0


    def generate_dataframe(self):
        data = []
        for obj in self.object_list:
            data.append([obj.creation_date,
                        obj.last_get_date,
                        obj.last_put_date,
                        obj.size_gb,
                        obj.current_tier,
                        obj.time_in_tier,
                        obj.transitions,
                        obj.id,
                        obj.get_count,
                        obj.put_count,
                        obj.list_count])
        return pd.DataFrame(data, columns = ["creation_date",
                                            "last_get_date",
                                            "last_put_date",
                                            "size_gb",
                                            "current_tier",
                                            "time_in_tier",
                                            "transitions",
                                            "id",
                                            "get_count",
                                            "put_count",
                                            "list_count"])

    def sum_df_by_date(self, df):
        grouped = df.groupby("creation_date")[["size_gb", "get_count", "put_count", "list_count"]].sum()
        print(grouped)
