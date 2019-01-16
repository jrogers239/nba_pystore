import requests
import pandas as pd
import json
import time
import os, sys
import pickle
import datetime as dt
from nba_api.stats.library import data
from nba_api.stats.endpoints import commonallplayers, commonplayerinfo, teamdetails, teaminfocommon

class player_ids(object):

    def __init__(self):
        self.ids = pd.DataFrame(data.players)
        self.ids = self.ids.loc[4373:,:]
        self.ids.columns = ['PERSON_ID', 'LAST_NAME', 'FIRST_NAME', 'DISPLAY_FIRST_LAST']
        self.path = os.path.dirname(__file__)
    # Updates
    def sync_players(self, season):
        # Grab Headers for
        headers = commonplayerinfo.CommonPlayerInfo(player_id='51').expected_data
        try:
            from json.decoder import JSONDecodeError
        except ImportError:
            JSONDecodeError = ValueError

        # Grab Player Info
        for header in headers['CommonPlayerInfo']:
            self.ids[header] = ""

        if os.path.exists(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle")):
            with open(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle"),'rb') as f:
                players = pickle.load(f)
        else:
            players = pd.DataFrame(columns=[self.ids.columns])

        for id in self.ids.loc[:,"PERSON_ID"]:
            # Sleep to avoid API constraints
            time.sleep(2)
            try:
                player = commonplayerinfo.CommonPlayerInfo(player_id=id).common_player_info.get_data_frame()
            except JSONDecodeError:
                print(id,'Decoder has failed')
            # If exists already - rewrite, if doesn't append
            if players["PERSON_ID"].isin([id]).any() == True:
                print("exists")
                players.loc[players["PERSON_ID"] == id] = player
            else:
                print(id)
                players = pd.concat([players,player])

        # Write to pickle afterwards
            with open(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle"),'wb') as f:
                pickle.dump(players,f)

        # Grab Common Players
        headers = commonallplayers.CommonAllPlayers(season=season).expected_data
        for header in headers['CommonAllPlayers']:
            self.ids[header] = ""

        with open(os.path.join(self.path,"data/CommonAllPlayers/playerlist.pickle"),'wb') as f:
            pickle.dump(players,f)

        common_players = commonallplayers.CommonAllPlayers(season=season).common_all_players.get_data_frame()

        #Combine for player_list_dim



        return dim_playlist

class team_ids(object):

    def __init__(self):
        self.ids = pd.DataFrame(data.teams)
        self.ids.columns = ['TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'YEAR_FOUNDED', 'CITY', 'FULL_TEAM_NAME', 'STATE']
        self.path = os.path.dirname(__file__)

    def sync_teams(self, season):

        if os.path.exists(os.path.join(self.path,"data/TeamDetails/teamlist.pickle")):
            with open(os.path.join(self.path,"data/TeamDetails/teamlist.pickle"),'rb') as f:
                teams_detail = pickle.load(f)
        else:
            headers = teamdetails.TeamDetails(team_id=1610612737).expected_data
            teams_detail = pd.DataFrame(columns=headers['TeamBackground'])

        for id in self.ids.loc[:,"TEAM_ID"]:
            # Sleep to avoid API constraints
            time.sleep(2)
            try:
                team = teamdetails.TeamDetails(team_id=id).team_background.get_data_frame()
            except JSONDecodeError:
                print(id,'Decoder has failed')
            # If exists already - rewrite, if doesn't append
            if teams_detail["TEAM_ID"].isin([id]).any() == True:
                print("exists")
                teams_detail.loc[teams_detail["TEAM_ID"] == id] = team
            else:
                print(id)
                teams_detail = pd.concat([teams_detail,team])

            with open(os.path.join(self.path,"data/TeamDetails/teamlist.pickle"),'wb') as f:
                pickle.dump(teams_detail,f)

        if os.path.exists(os.path.join(self.path,"data/TeamInfoCommon/teamlist.pickle")):
            with open(os.path.join(self.path,"data/TeamInfoCommon/teamlist.pickle"),'rb') as f:
                teams_common = pickle.load(f)
        else:
            headers = teaminfocommon.TeamInfoCommon(team_id=1610612737).expected_data
            teams_common = pd.DataFrame(columns=headers['TeamInfoCommon'])

        for id in self.ids.loc[:,"TEAM_ID"]:
            # Sleep to avoid API constraints
            time.sleep(2)
            try:
                team = teaminfocommon.TeamInfoCommon(team_id=id).team_info_common.get_data_frame()
            except JSONDecodeError:
                print(id,'Decoder has failed')
            # If exists already - rewrite, if doesn't append
            if teams_common["TEAM_ID"].isin([id]).any() == True:
                print("exists")
                teams_common.loc[teams_common["TEAM_ID"] == id] = team
            else:
                print(id)
                teams_common = pd.concat([teams_common,team])

            with open(os.path.join(self.path,"data/TeamInfoCommon/teamlist.pickle"),'wb') as f:
                pickle.dump(teams_common,f)

        return dim_teamlist

# print(team_ids().sync_teams('2017-18'))
# print(player_ids().sync_players('2017-18'))

ids = pd.DataFrame(data.teams)
print(ids)

path = os.path.dirname(__file__)

with open(os.path.join(path,"data/TeamDetails/teamlist.pickle"),'rb') as f:
    playlist = pickle.load(f)

print(playlist)

team = teamdetails.TeamDetails(team_id=1610612737).team_background.get_data_frame()
print(team)
#
# with open(os.path.join(path,"data/CommonPlayerInfo/playerlist.pickle"),'rb') as f:
#     playlist = pickle.load(f)
#
# ids = pd.DataFrame(data.players)
# player = commonplayerinfo.CommonPlayerInfo(player_id=1032).common_player_info.get_data_frame()
# print(playlist)
# print(ids.loc[1532:,:])
