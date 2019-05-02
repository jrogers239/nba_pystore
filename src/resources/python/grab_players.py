import requests
import pandas as pd
import json
import time
import os, sys
import pickle
import datetime as dt
import constants
import csv
from nba_api.stats.library import data
from nba_api.stats.endpoints import leaguegamelog, playercareerstats, playerawards, leaguedashplayerstats, leaguedashplayerclutch, leaguedashptstats, leaguedashptdefend, leaguedashplayerptshot, leaguedashplayershotlocations, leaguehustlestatsplayer, boxscorefourfactorsv2, boxscoreadvancedv2, boxscoremiscv2, boxscorescoringv2, boxscoreusagev2, boxscoreplayertrackv2

class player_career_stats(object):

    def __init__(self):
        self.path = os.path.dirname(__file__)
        self.columns = playercareerstats.PlayerCareerStats(player_id='52').expected_data['SeasonTotalsRegularSeason']
        self.league_id = constants.league.NBA
        self.current_season = constants.CURRENT_SEASON
    def grab_career_stats(self):

        try:
            from json.decoder import JSONDecodeError
        except ImportError:
            JSONDecodeError = ValueError

        if os.path.exists(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle")):
            with open(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle"),'rb') as f:
                players_id = pickle.load(f)

        if os.path.exists(os.path.join(self.path,"data/6_f_player_career_totals/fact_player_career_stats.pickle")):
            with open(os.path.join(self.path,"data/6_f_player_career_totals/fact_player_career_stats.pickle"),'rb') as f:
                info = pickle.load(f)
        else:
            info = pd.DataFrame(columns=[self.columns])

        # Include PLayers that are playing this year
        players = players_id.loc[players_id['TO_YEAR'] == int(constants.CURRENT_SEASON[:4])]

        # Exclude Players that haven't played in the NBA
        players = players.loc[players['NBA_FLAG'] == 'Y']

        for id in players['PERSON_ID']:
            # Add Regular Season, Playoffs Indicator to DataFrame

            try:
                fact_career_playoffs = playercareerstats.PlayerCareerStats(player_id=id,league_id_nullable=self.league_id).season_totals_post_season.get_data_frame()

                print(id,' ','Regular Season Completed')

                fact_career_regular_season = playercareerstats.PlayerCareerStats(player_id=id,league_id_nullable=self.league_id).season_totals_regular_season.get_data_frame()

                print(id,' ','Playoffs Completed')

                fact_career_regular_season['PLAYOFFS_INDICATOR'] = '0'
                fact_career_regular_season['REG_SEASON_INDICATOR'] = '1'

                fact_career_playoffs['PLAYOFFS_INDICATOR'] = '1'
                fact_career_playoffs['REG_SEASON_INDICATOR'] = '0'

            except JSONDecodeError:
                print(id,' ','Decoder has failed')
                with open(os.path.join(self.path,"data/6_f_player_career_totals/debug_ids.pickle"),'wb') as f:
                    pickle.dump(id,f)

            cols = fact_career_playoffs.columns.difference(fact_career_regular_season.columns)
            idx = pd.Index(['GAME_ID'])
            cols = cols.append(idx)

            player_career_stats = fact_career_regular_season.append(fact_career_playoffs)

            if info["PLAYER_ID"].isin([id]).any() == True:
                print(id,' ','Exists - Replacing')
                info.loc[info["PLAYER_ID"]  == id] = player_career_stats
            else:
                print(id,' ','New - Appending')
                info = pd.concat([info,player_career_stats])

            # Avoid API Timer
            with open(os.path.join(self.path,"data/6_f_player_career_totals/fact_player_career_stats.pickle"),'wb') as f:
                pickle.dump(info,f)

            time.sleep(7)


class player_games(object):
    # Add Regular Season, Playoffs and Pre-Season Indicator to DataFrame
    def __init__(self):
        self.path = os.path.dirname(__file__)
        # self.seasons = constants.se   ason.seasons
        self.league_id = constants.league.NBA

    def grab_p_games(self, season):

        # Regular Season
        fact_player_games_regular_season = leaguegamelog.LeagueGameLog(league_id=self.league_id, player_or_team_abbreviation="P", season_all_time=season,  season_type_all_star='Regular Season').get_data_frames()[0]

        fact_player_games_regular_season['PLAYOFFS_INDICATOR'] = '0'
        fact_player_games_regular_season['REG_SEASON_INDICATOR'] = '1'


        # Playoffs
        fact_player_games_playoffs = leaguegamelog.LeagueGameLog(league_id=self.league_id, player_or_team_abbreviation="P", season_all_time=season,  season_type_all_star='Playoffs').get_data_frames()[0]

        fact_player_games_playoffs['PLAYOFFS_INDICATOR'] = '1'
        fact_player_games_playoffs['REG_SEASON_INDICATOR'] = '0'

        cols = fact_player_games_playoffs.columns.difference(fact_player_games_regular_season.columns)
        idx = pd.Index(['GAME_ID'])
        cols = cols.append(idx)

        fact_player_games = fact_player_games_regular_season.append(fact_player_games_playoffs)

        # Expand with advanced stats later
        # for id in fact_player_games['GAME_ID']:

        return fact_player_games

class player_awards(object):

    def __init__(self):
        self.path = os.path.dirname(__file__)
        self.columns = playerawards.PlayerAwards(player_id='52').expected_data['PlayerAwards']
        self.league_id = constants.league.NBA

    def grab_awards(self, runtype):
        try:
            from json.decoder import JSONDecodeError
        except ImportError:
            JSONDecodeError = ValueError

        if os.path.exists(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle")):
            with open(os.path.join(self.path,"data/CommonPlayerInfo/playerlist.pickle"),'rb') as f:
                players_id = pickle.load(f)

        if os.path.exists(os.path.join(self.path,"data/7_f_player_awards/fact_player_awards.pickle")):
            with open(os.path.join(self.path,"data/7_f_player_awards/fact_player_awards.pickle"),'rb') as f:
                info = pickle.load(f)
        else:
            info = pd.DataFrame(columns=[self.columns])

        if runtype == "Y":
            # Include PLayers that are playing this year
            players = players_id.loc[players_id['TO_YEAR'] == int(constants.CURRENT_SEASON[:4])]

            # Exclude Players that haven't played in the NBA
            players = players.loc[players['NBA_FLAG'] == 'Y']
        elif runtype == "D":
        # # Exclude Players that have already run
            players = players_id[~players_id['PERSON_ID'].isin(info ['PERSON_ID'])]

        for id in players['PERSON_ID']:
            # Add Regular Season, Playoffs Indicator to DataFrame
            try:
                fact_awards = playerawards.PlayerAwards(player_id=id).player_awards.get_data_frame()
            except JSONDecodeError:
                print(id,' ','Decoder has failed')
                with open(os.path.join(self.path,"data/7_f_player_awards/debug_ids.pickle"),'wb') as f:
                    pickle.dump(id,f)

            if info["PERSON_ID"].isin([id]).any() == True:
                print(id,' ','Exists - Replacing')
                info.loc[info["PERSON_ID"]  == id] = fact_awards
            else:
                print(id,' ','New - Appending')
                info = pd.concat([info,fact_awards])

            with open(os.path.join(self.path,"data/7_f_player_awards/fact_player_awards.pickle"),'wb') as f:
                pickle.dump(info,f)

            time.sleep(4)

#
# class player_stats(object):
#
#         def __init__(self):
#             self.path = os.path.dirname(__file__)
#             self.columns = playerawards.PlayerAwards(player_id='52').expected_data['PlayerAwards']
#             self.league_id = constants.league.NBA
#
#         def grab_awards(self, runtype):
#
#             # Regular Season
#             fact_player_games_regular_season = leaguegamelog.LeagueGameLog(league_id=self.league_id, player_or_team_abbreviation="P", season_all_time=season,  season_type_all_star='Regular Season').get_data_frames()[0]
#
#             fact_player_games_regular_season['PLAYOFFS_INDICATOR'] = '0'
#             fact_player_games_regular_season['REG_SEASON_INDICATOR'] = '1'
#
#
#             # Playoffs
#             fact_player_games_playoffs = leaguegamelog.LeagueGameLog(league_id=self.league_id, player_or_team_abbreviation="P", season_all_time=season,  season_type_all_star='Playoffs').get_data_frames()[0]
#
#             fact_player_games_playoffs['PLAYOFFS_INDICATOR'] = '1'
#             fact_player_games_playoffs['REG_SEASON_INDICATOR'] = '0'
#
#             cols = fact_player_games_playoffs.columns.difference(fact_player_games_regular_season.columns)
#             idx = pd.Index(['GAME_ID'])
#             cols = cols.append(idx)
#
#             fact_player_games = fact_player_games_regular_season.append(fact_player_games_playoffs)
#
#
#                 time.sleep(3)
#
#
#     return fact_player_stats
#
# class player_clutch_stats(object):
#
#     return fact_player_clutch_stats
#
# class player_tracking_stats(object):
#
#     return fact_player_tracking_stats
#
# class player_defensive_stats(object):
#
#     return fact_defensive_stats
#
# class player_shooting_stats(object):
#
#     return fact_player_shooting_stats
#
# class player_shot_location_stats(object):
#
#     return fact_player_shot_location_stats
#
# class player_defensiveshot_location_stats(object):
#
#     return fact_player_defensiveshot_location_stats
#
# class player_hustle_stats(object):
#
#     return player_hustle_stats


# path = os.path.dirname(__file__)
# # if os.path.exists(os.path.join(path,"data/1_d_static_players/dim_playlist.pickle")):
# #     with open(os.path.join(path,"data/1_d_static_players/dim_playlist.pickle"),'rb') as f:
# #         players_id = pickle.load(f)
# #
# if os.path.exists(os.path.join(path,"data/6_f_player_career_totals/fact_player_career_stats.pickle")):
#     with open(os.path.join(path,"data/6_f_player_career_totals/fact_player_career_stats.pickle"),'rb') as f:
#         info = pickle.load(f)
# #
# #         uncommon = players_id[~players_id['PERSON_ID'].isin(info ['PLAYER_ID'])]
#
# if not os.path.exists(os.path.join(path,'data/6_f_player_career_totals/fact_player_career_stats.csv')):
#     info.to_csv(os.path.join(path,'data/6_f_player_career_totals/fact_player_career_stats.csv'))
#
# print(info)

# fact_awards = playerawards.PlayerAwards(player_id='2554').player_awards.get_data_frame()
# print(fact_awards)
# d = player_career_stats().grab_career_stats()
d = player_awards().grab_awards("D")
