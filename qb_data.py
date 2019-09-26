import pandas as pd
import numpy as np
import bs4 as bs
import requests
import time as t


def populate_team_dict(pass_defense_df, player_pool_df):
    ''' Function populate_team_dict
            Parameters: pass_defense_df and player_pool_df both pandas
            DataFrames
            Returns: team_dict, a dictionary with teams as keys and team
            abbreviations as values.
            Does: First, it converts the pass_defense_df's index to a list
            (team_list). Second, it maps the desired team abbreviations from
            team_abbreviations_dict to mod_player_pool_df. Third, it iterates
            through each team abbreviation in mod_player_pool_df, extracts
            each unique team abbreviation, and populates each unique team
            abbreviation in team_abbreviations_list. Finally, team_dict is
            initialized and returned.
            
    '''
    team_abbreviations_dict = {"SFO":"SF","KAN":"KC","NWE":"NE","NOR": "NO",\
                              "GNB": "GB", "TAM": "TB"}
    mod_player_pool_df = player_pool_df
    team_list = pass_defense_df.index.tolist()
    
    team_abbreviations_list = []
    for team in mod_player_pool_df["Team Abbr"].unique():
        team_abbreviations_list.append(team)
    # For any elements in team_abbreviations_list that are not keys in
    # team_abbreviations_dict, team_abbreviations_list is not modified.
    new_team_abbreviations_list = sorted([team_abbreviations_dict.get(i,i) for \
                                          i in team_abbreviations_list])

    team_dict = dict(zip(team_list, new_team_abbreviations_list))
    # This corrects the incorrect SF and SEA mapping 
    team_dict["San Francisco 49ers"] = "SF"
    team_dict["Seattle Seahawks"] = "SEA"
    
    return team_dict

def modify_player_pool(player_pool_df):
    ''' Function modify_player_pool
            Parameters: player_pool_df, a pandas DataFrame
            Returns: mod_player_pool_df, also a pandas DataFrame.
            Does: First, it modifies the player's name from last name
            , first name to first name, last name. Second, it maps team
            abbreviations by the desired abbreviations in
            team_abbreviations_dict. Third, it creates a column
            "Season_Week" which uniquely identifies the season and week. 
            Finally, it utilizes the .mask() method to represent the home
            team in a numerical format (1 if home team, 0 if away team).
    '''
    team_abbreviations_dict = {"SFO":"SF","KAN":"KC","NWE":"NE","NOR": "NO",\
                              "GNB": "GB", "TAM": "TB"}
    mod_player_pool_df = player_pool_df
    # Reverses player name
    mod_player_pool_df["New Name"] = mod_player_pool_df["Name"].str.split(",").\
                                     str[::-1].str.join(" ")
    mod_player_pool_df = mod_player_pool_df.drop(["Name"], axis = 1)
    mod_player_pool_df = mod_player_pool_df.set_index("New Name")
    mod_player_pool_df = mod_player_pool_df.rename(columns \
                                                   = {"Team": "Team Abbr", \
                                                      "Oppt": "Oppt Abbr"})
    # Formats all data to uppercase in the "Team" and "Oppt" columns.
    mod_player_pool_df["Team Abbr"] = mod_player_pool_df["Team Abbr"].str. \
                                      upper()
    mod_player_pool_df["Oppt Abbr"] = mod_player_pool_df["Oppt Abbr"].str. \
                                      upper()
    # The .fillna() method keeps team abbreviations intact for those team
    # abbreviations not in team_abbreviations_dict.
    mod_player_pool_df["Team Abbr"] = mod_player_pool_df["Team Abbr"]. \
                                      map(team_abbreviations_dict). \
                                      fillna(mod_player_pool_df["Team Abbr"])
    mod_player_pool_df["Oppt Abbr"] = mod_player_pool_df["Oppt Abbr"]. \
                                      map(team_abbreviations_dict). \
                                      fillna(mod_player_pool_df["Oppt Abbr"])
    # Inserts unique season week column
    mod_player_pool_df.insert(0,"Season_Week", mod_player_pool_df["Year"]. \
                              map(str) + "week" + mod_player_pool_df["Week"]. \
                              map(str))
    mod_player_pool_df = mod_player_pool_df.drop(["Year", "Week", "GID"], \
                                                 axis = 1)
    # Initializes the Boolean equalities for the .mask() method 
    home_mask = (mod_player_pool_df["h/a"] == "h")
    away_mask = (mod_player_pool_df["h/a"] == "a")
    # .mask() method replaces values when the condition specified is true and
    # skips over values when the condition specified is false.
    mod_player_pool_df["h/a"] = mod_player_pool_df["h/a"].mask(home_mask, 1)
    mod_player_pool_df["h/a"] = mod_player_pool_df["h/a"].mask(away_mask, 0)
    
    return mod_player_pool_df

def modify_pass_defense_stats(pass_defense_df):
    ''' Function modify_pass_defense_stats
            Parameters: pass_defense_df, a pandas DataFrame
            Returns: mod_pass_defense_df, also a pandas DataFrame
            Does: Sets the index of the DataFrame to the team and drops
            rows and columns that will not be utilized in subequent data
            analysis.
    '''
    mod_pass_defense_df = pass_defense_df.rename({"Tm":"Team"}, axis = 1)
    mod_pass_defense_df = mod_pass_defense_df.set_index("Team")
    mod_pass_defense_df = mod_pass_defense_df.drop(["Avg Team","League Total",\
                                                    "Avg Tm/G"])
    mod_pass_defense_df = mod_pass_defense_df.drop(["Rk","G", "Cmp", "Att",\
                                                    "TD","Int"], axis = 1)
    mod_pass_defense_df = mod_pass_defense_df.sort_index()
    
    return mod_pass_defense_df

def modify_vegas_data(vegas_df, team_dict):
    ''' Function merge_dataframes
            Parameters: vegas_df, a pandas DataFrame and
            team_dict, a dictionary.
            Returns: final_vegas_df, a pandas DataFrame
            Does: First, it creates a column "Season_Week" which uniquely
            identifies the season and week. Second, it creates two new
            columns "team_home_id" and "team_away_id", which are mapped
            by team_dict. Third, it utilizes the .mask() method to identify
            whether the home team or the away team is the favorite.
            Fourth, the spread and the game total are used to calculated the
            implied team totals for the favorite and the underdog. Fifth,
            favorite_df and underdog_df are created to facilitate future
            merging with player pool and NFL stats. Finally, favorite_df
            and underdog_df are concatenated.
    '''
    vegas_df.insert(3,"Season_Week", vegas_df["schedule_season"].map(str) + \
                    "week" + vegas_df["schedule_week"])
    vegas_df.insert(6, "team_home_id", vegas_df["team_home"].map(team_dict))
    vegas_df.insert(10, "team_away_id", vegas_df["team_away"].map(team_dict))
    vegas_df = vegas_df.drop(["schedule_date", "schedule_season", \
                              "schedule_week"], axis = 1)
    
    # Initializes the Boolean equality for the .mask() method 
    favorite_mask = (vegas_df["team_home_id"] == vegas_df["team_favorite_id"])
    # .mask() method replaces values when the condition specified is true.
    # This populates team_underdog_id based on whether or not the home team is
    #the favorite.
    vegas_df.insert(9, "team_underdog_id", vegas_df["team_home_id"]. \
                    mask(favorite_mask, vegas_df["team_away_id"]))
    # Calculates the individual team totals implied by the spread and game total
    # set by the sportsbooks.
    vegas_df["favorite_team_total"] = (vegas_df["over_under_line"] - \
                                       vegas_df["spread_favorite"]) / 2
    vegas_df["underdog_team_total"] = vegas_df["favorite_team_total"] + \
                                      vegas_df["spread_favorite"]
    favorite_df = vegas_df.filter(["Season_Week", "team_home_id", \
                                   "team_favorite_id", "spread_favorite", \
                                   "over_under_line", "favorite_team_total"], \
                                  axis = 1)
    underdog_df = vegas_df.filter(["Season_Week", "team_home_id", \
                                   "team_underdog_id", "spread_favorite", \
                                   "over_under_line", "underdog_team_total"], \
                                  axis = 1)
    underdog_df["spread_favorite"] = underdog_df["spread_favorite"] * -1
    favorite_df = favorite_df.rename(columns = {"team_favorite_id": \
                                                "Team Abbr", "spread_favorite":\
                                                "spread", "over_under_line": \
                                                "game total", \
                                                "favorite_team_total": \
                                                "team_total"})
    underdog_df = underdog_df.rename(columns = {"team_underdog_id": \
                                                "Team Abbr", "spread_favorite":\
                                                "spread", "over_under_line": \
                                                "game total", \
                                                "underdog_team_total": \
                                                "team_total"})
    final_vegas_df = pd.concat([favorite_df,underdog_df])
    final_vegas_df = final_vegas_df.sort_values(by = ["Season_Week"]).\
                     reset_index(drop = True)
    
    return final_vegas_df
    
def merge_dataframes(pass_defense_df, player_pool_df, vegas_df, team_dict):
    ''' Function merge_dataframes
            Parameters: pass_defense_df, player_pool_df, and vegas_df,
            all pandas DataFrames
            team_dict, a dictionary
            Returns: final_mod_df, a pandas DataFrame
            Does: First, it reverses the team_dict, swapping keys (team names)
            with values (team abbreviations) and saves the new keys and values
            in reverse_team_dict. Second, it converts team_dict into a pandas
            DataFrame. The DataFrame's index is populated with the
            team_dict's keys (team names) and the only column in the DataFrame
            is populated with team_dict's values (team abbreviations). Third,
            it merges pass_defense_df and the newly initialized team_dict_df
            by the DataFrames' indices. Fourth, it inserts two new columns
            ("Team" and "Oppt"), which are mapped by reverse_team_dict.
            Fifth, it merges mod_player_pool_df with mod_pass_defense_df.
            Finally, it merges final_mod_df with vegas_df.
    '''
    reverse_team_dict = {}
    
    # For loop that populates reverse_team_dict with team abbreviations as keys
    # and teams as values.
    for key, value in team_dict.items():
        reverse_team_dict[value] = key
        
    team_dict_df = pd.DataFrame.from_dict(team_dict, orient = "index"). \
                   rename(columns = {0:"Team Dict Abbr"})
    # Merge by indices.
    mod_pass_defense_df = pass_defense_df.merge(team_dict_df, left_index = \
                                                True, right_index = True)
    mod_player_pool_df = player_pool_df
    # Creates a new column with each player's name before merging to preserve
    # player_pool_df's index.
    mod_player_pool_df["Player Name"] = mod_player_pool_df.index
    mod_player_pool_df.insert(2,"Team", mod_player_pool_df["Team Abbr"]. \
                              map(reverse_team_dict))
    mod_player_pool_df.insert(5,"Oppt", mod_player_pool_df["Oppt Abbr"]. \
                              map(reverse_team_dict))
    # Merge by explicit columns.
    final_mod_df = mod_player_pool_df.merge(mod_pass_defense_df, \
                                            left_on = "Oppt Abbr", \
                                            right_on = "Team Dict Abbr")
    # Merge by explicit columns.
    final_mod_df = final_mod_df.merge(vegas_df, left_on = ["Season_Week", \
                                                           "Team Abbr"], \
                                      right_on = ["Season_Week", "Team Abbr"])
    final_mod_df = final_mod_df.set_index("Player Name")
    final_mod_df = final_mod_df.drop(["Team Dict Abbr"], axis = 1)
    return final_mod_df

def main():
    # Reads pass defense data
    pass_defense_df = pd.read_csv("/Users/Michael/Desktop/Datasets/"\
                                  "pass_defense_week_17.csv")
    # Reads player pool data
    # sep is an optional paramter telling pandas how to separate the data into
    # columns.
    player_pool_df = pd.read_csv("/Users/Michael/Desktop/Datasets/"\
                                 "fd_week_17.csv", sep = ";")
    # Calls modify_player_pool
    mod_player_pool_df = modify_player_pool(player_pool_df)
    # Calls modify_pass_defense_stats
    mod_pass_defense_df = modify_pass_defense_stats(pass_defense_df)
    # Calls populate_team_dict
    team_dict = populate_team_dict(mod_pass_defense_df, mod_player_pool_df)
    # Reads spread and total data.
    vegas_df = pd.read_csv("/Users/Michael/Desktop/Datasets/"\
                                "historical_spreads_totals.csv")
    # Calls modify_vegas_data
    mod_vegas_df = modify_vegas_data(vegas_df, team_dict)
    # Calls merge_dataframes
    final_df = merge_dataframes(mod_pass_defense_df, mod_player_pool_df, \
                                mod_vegas_df, team_dict)
    # Saves to csv
    #final_df.to_csv("/Users/Michael/Desktop/Datasets/final_csv.csv")
    print(final_df.head())

if __name__ == "__main__":
    main()
