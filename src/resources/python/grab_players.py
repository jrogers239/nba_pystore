import requests
import pandas as pd
import json
import time
import os, sys
import pickle
import datetime as dt
import constants
from nba_api.stats.library import data
from nba_api.stats.endpoints import leaguegamelog, playercareerstats, playerawards, leaguedashplayerstats, leaguedashplayerclutch, leaguedashptstats, leaguedashptdefend, leaguedashplayerptshot, leaguedashplayershotlocations, leaguehustlestatsplayer, boxscorefourfactorsv2, boxscoreadvancedv2, boxscoremiscv2, boxscorescoringv2, boxscoreusagev2, boxscoreplayertrackv2

class player_career_stats(object):

    def __init__(self):
        self.path = os.path.dirname(__file__)
        self.columns = playercareerstats.PlayerCareerStats(player_id='52').expected_data['SeasonTotalsRegularSeason']
        self.league_id = constants.league.NBA

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

        for id in players_id['PERSON_ID']:
            # Add Regular Season, Playoffs Indicator to DataFrame
            fact_career_regular_season = playercareerstats.PlayerCareerStats(player_id=id,league_id_nullable=self.league_id).season_totals_regular_season.get_data_frame()

            fact_career_regular_season['PLAYOFFS_INDICATOR'] = '0'
            fact_career_regular_season['REG_SEASON_INDICATOR'] = '1'

            print(id,' ','Regular Season Completed')
            try:
                fact_career_playoffs = playercareerstats.PlayerCareerStats(player_id=id,league_id_nullable=self.league_id).season_totals_post_season.get_data_frame()
            except JSONDecodeError:
                print(id,' Decoder has failed')

            print(id,' ','Playoffs Completed')

            fact_career_playoffs['PLAYOFFS_INDICATOR'] = '1'
            fact_career_playoffs['REG_SEASON_INDICATOR'] = '0'

            cols = fact_career_playoffs.columns.difference(fact_career_regular_season.columns)
            idx = pd.Index(['GAME_ID'])
            cols = cols.append(idx)

            player_career_stats = fact_career_regular_season.append(fact_career_playoffs)

            if info["PLAYER_ID"].isin([id]).any() == True:
                print(id, " exists")
                players_info.loc[players_info["PLAYER_ID"] == id] = player
            else:
                print(id)
                info = pd.concat([info,player_career_stats])

            # Avoid API Timer
            with open(os.path.join(self.path,"data/6_f_player_career_totals/fact_player_career_stats.pickle"),'wb') as f:
                pickle.dump(info,f)

            time.sleep(5)
            print(info)
        return fact_player_career_stats

class player_games(object):
    # Add Regular Season, Playoffs and Pre-Season Indicator to DataFrame
    def __init__(self):
        self.path = os.path.dirname(__file__)
        # self.seasons = constants.season.seasons
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

# class player_awards(object):
#
#     return fact_player_awards
#
# class player_stats(object):
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

d = player_career_stats().grab_career_stats()
