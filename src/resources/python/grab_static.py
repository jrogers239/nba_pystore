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
        self.ids = self.ids.loc[4372:,:]
        self.ids.columns = ['PERSON_ID', 'LAST_NAME', 'FIRST_NAME', 'DISPLAY_FIRST_LAST']
        self.path = os.path.dirname(__file__)
    # Updates
    def sync_players(self, season):
        # Grab Headers for
        headers = commonplayerinfo.CommonPlayerInfo(player_id='51').expected_data

        common_players = commonallplayers.CommonAllPlayers(season=season).common_all_players.get_data_frame()

        with open(os.path.join(self.path,"data/CommonAllPlayers/playerlist.pickle"),'wb') as f:
            pickle.dump(common_players,f)
            
        sids = common_players['PERSON_ID'].loc[common_players['ROSTERSTATUS'] == 1]
        try:
            from json.decoder import JSONDecodeError
        except ImportError:
            JSONDecodeError = ValueError

        if os.path.exists(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle")):
            with open(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle"),'rb') as f:
                players_info = pickle.load(f)
        else:
            players_info = pd.DataFrame(columns=[self.ids.columns])


        for id in sids:
            # Sleep to avoid API constraints
            time.sleep(2)
            try:
                player = commonplayerinfo.CommonPlayerInfo(player_id=id).common_player_info.get_data_frame()
            except JSONDecodeError:
                print(id,' = Decoder has failed')
            # If exists already - rewrite, if doesn't append
            if players_info["PERSON_ID"].isin([id]).any() == True:
                print(id, " exists")
                players_info.loc[players_info["PERSON_ID"] == id] = player
            else:
                print(id)
                players_info = pd.concat([players_info,player])

        # Write to pickle afterwards
            with open(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle"),'wb') as f:
                pickle.dump(players_info,f)

        # Grab Common Players
        headers = commonallplayers.CommonAllPlayers(season=season).expected_data

        # for header in headers['CommonAllPlayers']:
        #     self.ids[header] = ""



        #Combine for player_list_dim
        cols = players_info.columns.difference(common_players.columns)
        idx = pd.Index(['PERSON_ID'])
        cols = cols.append(idx)
        dim_playlist = pd.merge(common_players, players_info[cols], on='PERSON_ID', how='outer')

        # Dump pickle so that we can use it later if necessary
        with open(os.path.join(self.path,"data/1_d_static_players/dim_playlist.pickle"),'wb') as f:
            pickle.dump(dim_playlist,f)


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

            cols = teams_common.columns.difference(teams_detail.columns)
            idx = pd.Index(['TEAM_ID'])
            cols = cols.append(idx)
            dim_teamlist = pd.merge(teams_detail, teams_common[cols], on='TEAM_ID', how='outer')

            # Dump pickle so that we can use it later if necessary
            with open(os.path.join(self.path,"data/2_d_static_teams/dim_teamlist.pickle"),'wb') as f:
                pickle.dump(dim_teamlist,f)

        return dim_teamlist
#

# team_ids().sync_teams('2017-18')
player_ids().sync_players('2018-19')
path = os.path.dirname(__file__)

# common_players = commonallplayers.CommonAllPlayers(season='2018-19').common_all_players.get_data_frame()
#
# with open(os.path.join(path,"data/CommonAllPlayers/playerlist.pickle"),'wb') as f:
#     pickle.dump(common_players,f)
#
# sids = common_players['PERSON_ID'].loc[common_players['ROSTERSTATUS'] == 1]


print(sids)
# print(player_ids().sync_players('2017-18'))

# ids = pd.DataFrame(data.teams)
# print(ids)
#
#
#
# with open(os.path.join(path,"data/CommonAllPlayers/playerlist.pickle"),'rb') as f:
#     df1 = pickle.load(f)
#
# with open(os.path.join(path,"data/CommonPlayerInfo/playerlist.pickle"),'rb') as f:
#     df2= pickle.load(f)
# cols = df2.columns.difference(df1.columns)
# idx = pd.Index(['PERSON_ID'])
# cols = cols.append(idx)
# print(df2[cols])
# df = pd.merge(df1, df2[cols], on='PERSON_ID', how='outer')
# df1.to_csv(os.path.join(path,"data/CommonAllPlayers/playerlist.csv"))
# df2.to_csv(os.path.join(path,"data/CommonPlayerInfo/playerlist.csv"))
# df.to_csv(os.path.join(path,"data/1_d_static_players/playerlist.csv"))
# # print(df)
#
# team = teamdetails.TeamDetails(team_id=1610612737).team_background.get_data_frame()
# print(team)
#
# with open(os.path.join(path,"data/CommonPlayerInfo/playerlist.pickle"),'rb') as f:
#     playlist = pickle.load(f)
#
# ids = pd.DataFrame(data.players)
# player = commonplayerinfo.CommonPlayerInfo(player_id=1032).common_player_info.get_data_frame()
# print(playlist)
# print(ids.loc[1532:,:])
