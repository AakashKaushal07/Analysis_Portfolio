# Django Items

# Modules
import pandas as pd
import numpy as np
import os

# Helper Functions
from metrics.helpers.infered_carry import infer_carries_with_confidence
from base_app.helpers import log_exception,get_logger

class AttackPhase:
    def __init__(self,path):
        self.df = pd.read_excel(path)
        self.game_id = os.path.split(path)[1].replace('.xlsx','')
        self.events_raw = self.__remove_extra_events()
        self.xt_grid = self.__get_xt_grid()
        self.infered_carries = infer_carries_with_confidence(self.events_raw,min_dist=5).rename(columns={'start_x' : 'x',"start_y" : "y"})
        # xT enabled DataFrames
        self.events = self.__enable_bins_and_add_xt_columns(self.events_raw)
        self.carries = self.__enable_bins_and_add_xt_columns(self.infered_carries)
        
    
    def __remove_extra_events(self) :
        return self.df[~self.df['event_type'].astype(str).isin(['Team set up','Injury Time Announcement',
                                                          'End', 'Collection End' ,'nan','Start','Collection End',
                                                          'Deleted event',"Start delay","End delay",'Player Off',
                                                          'Player on','Offside Pass'])]
    
    def __get_xt_grid(self) :
        return np.array([
            [0.00638303, 0.00779616, 0.00844854, 0.00977659, 0.01126267, 0.01248344, 0.01473596, 0.0174506 , 0.02122129, 0.02756312, 0.03485072, 0.0379259 ],
            [0.00750072, 0.00878589, 0.00942382, 0.0105949 , 0.01214719, 0.0138454 , 0.01611813, 0.01870347, 0.02401521, 0.02953272, 0.04066992, 0.04647721],
            [0.0088799 , 0.00977745, 0.01001304, 0.01110462, 0.01269174, 0.01429128, 0.01685596, 0.01935132, 0.0241224 , 0.02855202, 0.05491138, 0.06442595],
            [0.00941056, 0.01082722, 0.01016549, 0.01132376, 0.01262646, 0.01484598, 0.01689528, 0.0199707 , 0.02385149, 0.03511326, 0.10805102, 0.25745362],
            [0.00941056, 0.01082722, 0.01016549, 0.01132376, 0.01262646, 0.01484598, 0.01689528, 0.0199707 , 0.02385149, 0.03511326, 0.10805102, 0.25745362],
            [0.0088799 , 0.00977745, 0.01001304, 0.01110462, 0.01269174, 0.01429128, 0.01685596, 0.01935132, 0.0241224 , 0.02855202, 0.05491138, 0.06442595],
            [0.00750072, 0.00878589, 0.00942382, 0.0105949 , 0.01214719, 0.0138454 , 0.01611813, 0.01870347, 0.02401521, 0.02953272, 0.04066992, 0.04647721],
            [0.00638303, 0.00779616, 0.00844854, 0.00977659, 0.01126267, 0.01248344, 0.01473596, 0.0174506 , 0.02122129, 0.02756312, 0.03485072, 0.0379259 ]
        ])
        
    def __enable_bins_and_add_xt_columns(self,df) :
        # 1. Create temporary Series
        temp_x = df['x'].copy().fillna(-1)
        temp_y = df['y'].copy().fillna(-1)
        temp_end_x = df['end_x'].copy().fillna(-1)
        temp_end_y = df['end_y'].copy().fillna(-1)
        
        # 2. Define "Out of Bounds" mask (outside 0-100)
        # This identifies both NaNs (which we set to -1) and actual out-of-bounds values
        is_invalid_start = (df['x'] <= 0) | (df['x'] > 100) | (df['y'] <= 0) | (df['y'] > 100) | df['x'].isna()
        is_invalid_end = (df['end_x'] <= 0) | (df['end_x'] > 100) | (df['end_y'] <= 0) | (df['end_y'] > 100) | df['end_x'].isna()
        
        # 3. Calculate indices using clip (to prevent index errors during the lookup)
        df.loc[:, 'x_bin'] = np.clip((temp_x * 12 / 100).astype(int), 0, 11)
        df.loc[:, 'y_bin'] = np.clip((temp_y * 8 / 100).astype(int), 0, 7)
        df.loc[:, 'end_x_bin'] = np.clip((temp_end_x * 12 / 100).astype(int), 0, 11)
        df.loc[:, 'end_y_bin'] = np.clip((temp_end_y * 8 / 100).astype(int), 0, 7)
        
        # 4. Map values from the grid
        df.loc[:, 'start_xT'] = self.xt_grid[df['y_bin'], df['x_bin']]
        df.loc[:, 'end_xT'] = self.xt_grid[df['end_y_bin'], df['end_x_bin']] # Corrected indexing
        
        # 5. Apply the 0.0 value to all invalid rows (NaN or Out of Bounds)
        df.loc[is_invalid_start, ['start_xT','x_bin','y_bin']] = [0.0,None,None]
        df.loc[is_invalid_end, ['end_xT','end_x_bin','end_y_bin']] = [0.0,None,None]
        
        # 6. Calculate Difference
        df.loc[:, 'diff_xT'] = df['end_xT'] - df['start_xT']
        return df  

    ## 1. Field Tilt
    def get_field_tilt_values(self):
        '''
        Calculate the ratio of successful action in offensive half of each team
        
        Returns:
            dict : containing team name as key an field tilt as value
        '''
        final_3rd_actions = self.events[(self.events['x'] >=66.0) & (self.events['outcome'] == 'Successful') ]

        valid_actions = ["Pass","Ball recovery","Ball touch","Take On","Corner Awarded","Miss","Foul","Aerial","Goal","Interception"]
        team_actions = final_3rd_actions.loc[final_3rd_actions['event_type'].isin(valid_actions)].copy()
        total_actions = team_actions.shape[0]
        team_wise_actions = [{'game_id' : self.game_id, 'team' : k, 'field_tilt' : round((v/total_actions)*100,2)} for k,v in team_actions['team'].value_counts().to_dict().items()]
        return pd.DataFrame(team_wise_actions)
    
    ## 2. Final third Entries
    def get_final_third_entires(self):
        '''
        Computes the Xt generated by succcessfuly enty into final third.
        
        returns:
            Dataframe : overall : aggeregated team stats
            Dataframe : player_wise : playerwise stats
        '''
        final_third_entries_pass = self.events.loc[(self.events['x'] < 66) & (self.events['end_x'] >=66)].copy()
        final_third_entries_carry = self.carries.loc[(self.carries['x'] < 66) & (self.carries['end_x'] >=66)].copy()
        
        overall_pass = final_third_entries_pass.groupby(['team']).agg(
                xT_pass=('diff_xT', 
                        lambda x: x[final_third_entries_pass.loc[x.index, 'outcome'] == 'Successful'].sum()),
                total_actions=('outcome', "count"),
                successful_action=('outcome', lambda x: (x == 'Successful').sum()),
                failed_actions=('outcome', lambda x: (x != 'Successful').sum()),
        )
        overall_carry = final_third_entries_carry.groupby(['team']).agg(
                xT_carry=('diff_xT', "sum"),
                total_carries=('diff_xT', "count")
        )
        
        player_wise_pass = final_third_entries_pass.groupby(['team','player_name','player_id']).agg(
                xT_pass=('diff_xT', 
                        lambda x: x[final_third_entries_pass.loc[x.index, 'outcome'] == 'Successful'].sum()),
                total_actions=('outcome', "count"),
                successful_action=('outcome', lambda x: (x == 'Successful').sum()),
                failed_actions=('outcome', lambda x: (x != 'Successful').sum()),
        )
        player_wise_carry = final_third_entries_carry.groupby(['team','player_name']).agg(
                xT_carry=('diff_xT', "sum"),
                total_carries=('diff_xT', "count")
        )
        overall =  pd.merge(
            overall_pass,
            overall_carry,
            left_index=True,
            right_index=True,
            how='outer'
        ).fillna(0.0)

        player_wise = pd.merge(
            player_wise_pass,
            player_wise_carry,
            left_index=True,
            right_index=True,
            how='outer'
        ).fillna(0.0)

        overall.loc[:, 'Total_xT'] = (overall['xT_pass'] + overall['xT_carry'])
        overall.loc[:, 'xT_per_action'] = (overall['xT_pass'] / overall['total_actions'])
        player_wise.loc[:, 'Total_xT'] = (player_wise['xT_pass'] + player_wise['xT_carry'])
        return overall,player_wise

    ## 3. xT Generated and Distribution
    def get_pass_total_xt(self) :

        valid_actions = ["Pass", "Ball touch", "Take On"]
        
        xt_df = self.events[(self.events['event_type'].isin(valid_actions)) &(self.events['outcome'] == 'Successful')]
        xt_df['zone'] = 1
        xt_df.loc[xt_df['x'] < 33, 'zone'] = 0
        xt_df.loc[xt_df['x'] >= 66, 'zone'] = 2
        
        return xt_df.groupby('team').agg(
        total_actions=('diff_xT', 'count'),
    
        total_pos_actions=('diff_xT', lambda x: (x > 0).sum()),
        total_neg_actions=('diff_xT', lambda x: (x < 0).sum()),
        pos_def_action=('diff_xT', lambda x: x[(x > 0) & (xt_df.loc[x.index, 'zone'] == 0)].count()),
        neg_def_action=('diff_xT', lambda x: x[(x < 0) & (xt_df.loc[x.index, 'zone'] == 0)].count()),
        pos_mid_action=('diff_xT', lambda x: x[(x > 0) & (xt_df.loc[x.index, 'zone'] == 1)].count()),
        neg_mid_action=('diff_xT', lambda x: x[(x < 0) & (xt_df.loc[x.index, 'zone'] == 1)].count()),
        pos_att_action=('diff_xT', lambda x: x[(x > 0) & (xt_df.loc[x.index, 'zone'] == 2)].count()),
        neg_att_action=('diff_xT', lambda x: x[(x < 0) & (xt_df.loc[x.index, 'zone'] == 2)].count()),   
        total_xT=('diff_xT', 'sum'),
        pos_xT=('diff_xT', lambda x: x[x > 0].sum()),
        neg_xT=('diff_xT', lambda x: x[x < 0].sum()),
        pos_def_xt=('diff_xT', lambda x: x[(x > 0) & (xt_df.loc[x.index, 'zone'] == 0)].sum()),
        neg_def_xt=('diff_xT', lambda x: x[(x < 0) & (xt_df.loc[x.index, 'zone'] == 0)].sum()),
        pos_mid_xt=('diff_xT', lambda x: x[(x > 0) & (xt_df.loc[x.index, 'zone'] == 1)].sum()),
        neg_mid_xt=('diff_xT', lambda x: x[(x < 0) & (xt_df.loc[x.index, 'zone'] == 1)].sum()),
        pos_att_xt=('diff_xT', lambda x: x[(x > 0) & (xt_df.loc[x.index, 'zone'] == 2)].sum()),
        neg_att_xt=('diff_xT', lambda x: x[(x < 0) & (xt_df.loc[x.index, 'zone'] == 2)].sum()),
        
    )
        
    ## 4. Progression Routes
    def __assign_zone(self,x, y):
        """
        Spatial binning with variable vertical resolution:
        - Defensive & Middle thirds: Left / Central / Right
        - Final third: Wing / Half-space / Central
        """

        # --- Determine third ---
        if x < 33:
            third = "def"
        elif x < 66:
            third = "mid"
        else:
            third = "final"

        # --- Vertical bins ---
        if third in {"def", "mid"}:
            # coarse lanes
            if y < 33:
                lane = "left"
            elif y < 66:
                lane = "central"
            else:
                lane = "right"
        else:  # final third → full resolution
            if y < 20:
                lane = "left_wing"
            elif y < 40:
                lane = "left_half_space"
            elif y < 60:
                lane = "central"
            elif y < 80:
                lane = "right_half_space"
            else:
                lane = "right_wing"
        
        return f"{third}_{lane}"    

    def __process_actions(self,df, min_dx):
        rows = []
        for _, r in df.iterrows():
            start_zone = self.__assign_zone(r["x"], r["y"])
            end_zone = self.__assign_zone(r["end_x"], r["end_y"])

            # Remove defensive-third recycling
            if start_zone.startswith("def_") and end_zone.startswith("def_"):
                continue
            # Enforce forward progression
            if r["end_x"] - r["x"] <= min_dx:
                continue
            # Ignore no spatial change
            if start_zone == end_zone:
                continue
            rows.append({
                "team": r["team"],
                "start_zone": start_zone,
                "end_zone": end_zone
            })
        return rows
    
    def get_progression_df(self, min_dx=5):
        """
        Build a progression DataFrame using adaptive spatial bins.
        """

        rows = []
        # ---- Passes ----
        passes = self.events[
            (self.events["event_type"] == "Pass") &
            (self.events["outcome"] == "Successful")
        ]
        rows.extend(self.__process_actions(passes,min_dx))

        # ---- Carries ----
        rows.extend(self.__process_actions(self.carries,min_dx))

        prog_df = pd.DataFrame(rows)

        summary = (
            prog_df
            .groupby(["team", "start_zone", "end_zone"])
            .size()
            .reset_index(name="count")
        )

        summary["pct_of_team"] = (
            summary["count"] /
            summary.groupby("team")["count"].transform("sum") * 100
        ).round(2)

        return summary

    def get_routes_covering_threshold(self,progression_df, threshold=60):
        """
        Returns progression routes per team until cumulative pct_of_team
        crosses the given threshold.
        """

        results = []

        for team, g in progression_df.groupby("team"):
            g = g.sort_values("pct_of_team", ascending=False).reset_index(drop=True)
            g["cum_pct"] = g["pct_of_team"].cumsum()

            # take routes until threshold is crossed
            cutoff_idx = g[g["cum_pct"] >= threshold].index.min()
            selected = g.loc[:cutoff_idx]

            results.append(selected)

        return pd.concat(results, ignore_index=True)

    ## 5. Verticality Index
    def get_verticality_with_actions(self,min_x=20):

        # Filter to successful passes in attacking phase
        passes = self.events[
            (self.events["event_type"] == "Pass") &
            (self.events["outcome"] == "Successful") &
            (self.events["x"] >= min_x)
        ].copy()

        # Forward distance
        passes["dx"] = passes["end_x"] - passes["x"]

        # Flag verticality
        passes["is_vertical"] = passes["dx"] > 2.5

        # Team-level verticality index
        verticality_index = (
            passes
            .groupby("team")
            .agg(
                vericalty_index = ("is_vertical","mean"),
                vertical_actions=("is_vertical", "sum"),     
                non_vertical_actions=("is_vertical", lambda x: (~x).sum()),
                
                avg_vertical_dist=("dx", lambda x: x[x > 2.5].mean()),
                median_vertical_dist=("dx",lambda x: x[x > 2.5].median()),
                max_vertical_dist=("dx",lambda x: x[x > 2.5].max()),
                
                avg_start_position=("x", lambda x: x[x > 20].mean()),
                median_start_position=("x",lambda x: x[x > 20].median()),

                avg_end_position=("end_x", lambda x: x[x > 20].mean()),
                median_end_position=("end_x",lambda x: x[x > 20].median()),
            )
        )

        return verticality_index

    ## 6. Width of Attack
    def get_offensive_width(self) : 
        OFFENSIVE_EVENTS = {
            "Pass",
            "Take On",
            "Shot",
            "Goal",
            "Miss",
            "Attempt Saved"
        }
        offensive_df = self.events[
            (
                self.events["event_type"].isin(OFFENSIVE_EVENTS)
            ) &
            (
                (self.events["outcome"] == "Successful") |
                (self.events["event_type"].isin({"Shot", "Goal", "Miss", "Attempt Saved"}))
            )
        ].copy()
        
        bins = [0, 20, 40, 60, 80, 100]
        labels = [
            "left_wing",
            "left_half_space",
            "central",
            "right_half_space",
            "right_wing"
        ]
        offensive_df["region"] = pd.cut(
            offensive_df["end_y"].fillna(offensive_df["y"]),
            bins=bins,
            labels=labels,
            include_lowest=True
        )
        bins = [0, 50, 100]
        labels = [
        "left","right"
        ]
        offensive_df["side"] = pd.cut(
            offensive_df["end_y"].fillna(offensive_df["y"]),
            bins=bins,
            labels=labels,
            include_lowest=True
        )
        width_usage = (
            offensive_df
            .groupby(["team", "region","side"],observed=True)
            .size()
            .reset_index(name="actions")
        )
        width_usage["region_presence"] = round((width_usage['actions'] /  width_usage.groupby("team",observed=True)["actions"].transform("sum"))*100,3)
        width_usage["side_presence"] = round((width_usage['actions'] /  width_usage.groupby(["team","side"],observed=True)["actions"].transform("sum"))*100,3)
        return width_usage

    ## 7. Centrality of Attack
    def get_centrality_data(self):
        """
        Computes final-third half-space usage (left/right),
        Zone 14 usage, and Zone 14 xG / xGOT per team.
        """

        OFFENSIVE_EVENTS = {
            "Pass",
            "Carry",
            "Take On",
            "Shot",
            "Goal",
            "Miss",
            "Attempt Saved"
        }

        # --- Filter final-third offensive actions ---
        ft_1 = self.events[
            (self.events["event_type"].isin(OFFENSIVE_EVENTS)) &
            (
                (self.events["outcome"] == "Successful") |
                (self.events["event_type"].isin({"Shot", "Goal", "Miss", "Attempt Saved"}))
            )
        ].copy()
        
        ft = pd.concat([ft_1,self.carries])
        
        ft["end_x_use"] = ft["end_x"].fillna(ft["x"])
        ft["end_y_use"] = ft["end_y"].fillna(ft["y"])

        ft = ft[ft["end_x_use"] >= 66]

        # --- Spatial flags ---
        ft["left_half_space"] = ft["end_y_use"].between(20, 40)
        ft["right_half_space"] = ft["end_y_use"].between(60, 80)

        ft["any_half_space"] = ft["left_half_space"] | ft["right_half_space"]

        ft["zone14"] = (
            ft["end_x_use"].between(66, 83) &
            ft["end_y_use"].between(40, 60)
        )
        ft["zone14_xG"] = ft["xG"].where(ft["zone14"])
        ft["zone14_xGOT"] = ft["xGOT"].where(ft["zone14"])

        # --- Aggregate per team ---
        summary = (
            ft
            .groupby("team")
            .agg(
                total_final_third_actions=("team", "count"),
        
                left_half_space_actions=("left_half_space", "sum"),
                right_half_space_actions=("right_half_space", "sum"),
                total_half_space_actions=("any_half_space", "sum"),
        
                zone14_actions=("zone14", "sum"),
                zone14_xG=("zone14_xG", "sum"),
                zone14_xGOT=("zone14_xGOT", "sum"),
            )
            .reset_index()
        )

        summary["left_half_space_pct"] = (
            summary["left_half_space_actions"] /
            summary["total_final_third_actions"] * 100
        ).round(2)

        summary["right_half_space_pct"] = (
            summary["right_half_space_actions"] /
            summary["total_final_third_actions"] * 100
        ).round(2)

        summary["half_space_pct"] = (
            summary["total_half_space_actions"] /
            summary["total_final_third_actions"] * 100
        ).round(2)

        summary["zone14_pct"] = (
            summary["zone14_actions"] /
            summary["total_final_third_actions"] * 100
        ).round(2)

        return summary     
    
    ## 8. Shooting
    def __aggregate(self,shots,group_col):
        summary = (
            shots
            .groupby(group_col)
            .agg(
                shots=("event_type", "count"),
                goals=("is_goal", "sum"),
                own_goals=("own_goal", "sum"),
                shots_on_target=("on_target", "sum"),
                xG=("xG", "sum"),
                xGOT=("xGOT", "sum")
            )
            .reset_index()
        )
        if group_col == 'team':
            summary["goals_prevented"] = (summary["xGOT"] - summary["goals"]).round(3)
        else:
            summary["goals_added"] = (summary["xGOT"] - summary["xG"]).round(3)
        summary["shot_accuracy"] = (
            summary["shots_on_target"] / summary["shots"]
        ).round(3)

        summary["xG_per_shot"] = (
            summary["xG"] / summary["shots"]
        ).round(3)

        summary["xGOT_per_SOT"] = (
            summary["xGOT"] / summary["shots_on_target"]
        ).replace([np.inf, -np.inf], 0).round(3)

        return summary

    def aggregate_shots_team_and_player(self):
        """
        Aggregates shot metrics at both team and player level.
        """

        SHOT_EVENTS = {"Shot", "Goal", "Miss", "Attempt Saved","Post"}

        shots = self.events[self.events["event_type"].isin(SHOT_EVENTS)].copy()

        # --- Flags ---
        shots["is_goal"] = (shots["event_type"] == "Goal") & (shots["own_goal"] ==False)
        shots["is_own_goal"] = shots["own_goal"]
        shots["on_target"] = shots["event_type"].isin({"Goal", "Attempt Saved"})


        team_summary = self.__aggregate(shots,"team")
        player_summary = self.__aggregate(shots,["player_name","player_id"])
        return team_summary, player_summary

