"""Wyscout event stream data to SPADL converter."""
from typing import Any, Dict, List, Optional, Set

import pandas as pd  # type: ignore
from pandas import Series
from pandera.typing import DataFrame

from . import config as spadlconfig

from .base import (
    _add_dribbles,
    _fix_clearances,
    _fix_direction_of_play,
    min_dribble_length,
)

from .schema import SPADLSchema

# from socceraction.spadl.schema import SPADLSchema

###################################
# WARNING: HERE BE DRAGONS
# This code for converting wyscout data was organically grown over a long period of time.
# It works for now, but needs to be cleaned up in the future.
# Enter at your own risk.
###################################

# def convert_to_actions(events: pd.DataFrame, home_team_id: int) -> pd.DataFrame:
def convert_to_actions(events: pd.DataFrame) -> pd.DataFrame:
    # -> DataFrame[SPADLSchema]:
    """
    Convert Wyscout events to SPADL actions.
    Parameters
    ----------
    events : pd.DataFrame
        DataFrame containing Wyscout events from a single game.
    home_team_id : int
        ID of the home team in the corresponding game.
    Returns
    -------
    actions : pd.DataFrame
        DataFrame with corresponding SPADL actions.
    """
    # events = pd.concat([events, get_tagsdf(events)], axis=1)
    events = make_new_positions(events)
    events = fix_wyscout_events(events)
    actions = create_df_actions(events)
    actions = fix_actions(actions)
    actions = _fix_direction_of_play(actions)
    actions = _fix_clearances(actions)
    actions["action_id"] = range(len(actions))
    # actions = _add_dribbles(actions)

    return events
    # events.pipe(DataFrame[SPADLSchema])


# def _get_tag_set(tags: List[Dict[str, Any]]) -> Set[int]:
#     return {tag["id"] for tag in tags}
#
#
# def get_tagsdf(events: pd.DataFrame) -> pd.DataFrame:
#     """Represent Wyscout tags as a boolean dataframe.
#     Parameters
#     ----------
#     events : pd.DataFrame
#         Wyscout event dataframe
#     Returns
#     -------
#     pd.DataFrame
#         A dataframe with a column for each tag.
#     """
#


def _make_position_vars(events: pd.DataFrame) -> Series:
    if events["pass_height"] == "blocked":
        start_x = end_x = events["location_x"]
        start_y = end_y = events["location_y"]
    elif events["type_primary"] in ["pass", "clearance", "throw_in", "interception",
                                    "goal_kick", "free_kick", "corner",
                                    "fairplay"]:
        start_x = events["location_x"]
        start_y = events["location_y"]
        end_x = events["pass_end_location_x"]
        end_y = events["pass_end_location_y"]
    elif events["type_primary"] in ["touch", "duel", "acceleration", "goalkeeper_exit"]:
        if events["type_carry"] == 1:
            start_x = events["location_x"]
            start_y = events["location_y"]
            end_x = events["carry_end_location_x"]
            end_y = events["carry_end_location_y"]
        else:
            start_x = events["location_x"]
            start_y = events["location_y"]
            end_x = None
            end_y = None
    else:
        start_x = events["location_x"]
        start_y = events["location_y"]
        end_x = None
        end_y = None
    return pd.Series([events['id'], start_x, start_y, end_x, end_y])


def make_new_positions(events: pd.DataFrame) -> pd.DataFrame:
    """Extract the start and end coordinates for each action.
    Parameters
    ----------
    events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with start and end coordinates for each action.
    """
    new_positions = events.apply(
        lambda x: _make_position_vars(x), axis=1
    )
    new_positions.columns = ["id", "start_x", "start_y", "end_x", "end_y"]
    events = pd.merge(events, new_positions, left_on="id", right_on="id")
    events[["start_x", "end_x"]] = events[["start_x", "end_x"]]
    events[["start_y", "end_y"]] = events[["start_y", "end_y"]]
    # events = events.drop("positions", axis=1)
    return events


def fix_wyscout_events(df_events: pd.DataFrame) -> pd.DataFrame:
    """Perform some fixes on the Wyscout events such that the spadl action dataframe can be built.
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with an extra column 'offside'
    """
    df_events = create_shot_coordinates(df_events)
    df_events = add_expected_assists(df_events)
    df_events = convert_duels(df_events)
    # df_events = convert_duels_success(df_events)
    df_events = insert_interception_coordinates(df_events)
    df_events = add_offside_variable(df_events)
    df_events = convert_touches(df_events)
    df_events = convert_accelerations(df_events)
    df_events = insert_fairplay_coordinates(df_events)
    df_events = insert_coordinates_edge_cases(df_events)
    # df_events = convert_simulations(df_events)
    return df_events




def create_shot_coordinates(df_events: pd.DataFrame) -> pd.DataFrame:
    """Create shot coordinates (estimates) from the Wyscout tags.
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with end coordinates for shots
    """
    goal_center_idx = df_events["shot_goal_zone"].isin(["gt", "gc", "gb"])
    df_events.loc[goal_center_idx, "end_x"] = 100.0
    df_events.loc[goal_center_idx, "end_y"] = 50.0

    goal_right_idx = df_events["shot_goal_zone"].isin(["gtr", "gr", "gbr"])
    df_events.loc[goal_right_idx, "end_x"] = 100.0
    df_events.loc[goal_right_idx, "end_y"] = 55.0

    goal_left_idx = df_events["shot_goal_zone"].isin(["gtl", "gl", "glb"])
    df_events.loc[goal_left_idx, "end_x"] = 100.0
    df_events.loc[goal_left_idx, "end_y"] = 45.0

    out_center_idx = df_events["shot_goal_zone"].isin(["ot", "pt"])
    df_events.loc[out_center_idx, "end_x"] = 100.0
    df_events.loc[out_center_idx, "end_y"] = 50.0

    out_right_idx = df_events["shot_goal_zone"].isin(["otr", "or", "obr"])
    df_events.loc[out_right_idx, "end_x"] = 100.0
    df_events.loc[out_right_idx, "end_y"] = 60.0

    out_left_idx = df_events["shot_goal_zone"].isin(["otl", "ol", "olb"])
    df_events.loc[out_left_idx, "end_x"] = 100.0
    df_events.loc[out_left_idx, "end_y"] = 40.0

    post_left_idx = df_events["shot_goal_zone"].isin(["ptl", "pl", "plb"])
    df_events.loc[post_left_idx, "end_x"] = 100.0
    df_events.loc[post_left_idx, "end_y"] = 55.38

    post_right_idx = df_events["shot_goal_zone"].isin(["ptr", "pr", "pbr"])
    df_events.loc[post_right_idx, "end_x"] = 100.0
    df_events.loc[post_right_idx, "end_y"] = 44.62

    blocked_idx = df_events["shot_goal_zone"] == "bc"
    # ? could add location of next action as block location
    df_events.loc[blocked_idx, "end_x"] = df_events.loc[blocked_idx, "start_x"]
    df_events.loc[blocked_idx, "end_y"] = df_events.loc[blocked_idx, "start_y"]

    return df_events


def add_expected_assists(df_events: pd.DataFrame) -> pd.DataFrame:
    """Add expected assist value for passes by taking xG on resulting shots
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with xA
    """
    next_event = df_events.shift(-1)

    selector_shot_assist = (df_events["type_shot_assist"] == 1)

    df_events.loc[selector_shot_assist, "metric_xa"] = next_event["shot_xg"]

    return df_events


def convert_duels(df_events: pd.DataFrame) -> pd.DataFrame:
    """Convert duel events.

    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with duels_success/duels_fail columns as well
        as type_primary's for dribble and take_on. Also, adds end_x and end_y for dribbles/take_ons
    """
    next_event = df_events.shift(-1)
    next_event2 = df_events.shift(-2)
    # Duels and Dribbles
    selector_duel = (df_events["type_primary"] == "duel")
    selector_dribble = (df_events["ground_duel_duel_type"] == "dribble")
    selector_take_on = (df_events["ground_duel_take_on"] == 1.0) & (df_events["ground_duel_duel_type"] == "dribble")

    # select next event related duel (ground duel and aerial duel)
    selector_related_duel_next = (df_events["ground_duel_related_duel_id"] == next_event["id"]) | \
                                 (df_events["aerial_duel_related_duel_id"] == next_event["id"])

    # if next_event or next_event2 is the other team, then flip the coordinates
    selector_same_team_next_1 = (df_events["team_id"] == next_event["team_id"])
    selector_same_team_next_2 = (df_events["team_id"] == next_event2["team_id"])

    # some events are already tagged as carry and have carry_end_location, keep as is
    selector_carry = (df_events["type_carry"] == 1)

    # Duels (Success)
    selector_duel_won = (df_events['ground_duel_kept_possession'] == 1.0) | \
                        (df_events['ground_duel_recovered_possession'] == 1.0) | \
                        (df_events['aerial_duel_first_touch'] == 1.0) | \
                        (df_events['ground_duel_progressed_with_ball'] == 1.0) | \
                        (df_events['ground_duel_stopped_progress'] == 1.0)

    # duel result
    df_events.loc[selector_duel & selector_duel_won, "duel_success"] = True
    df_events.loc[selector_duel & selector_duel_won, "duel_failure"] = False
    df_events.loc[selector_duel & ~selector_duel_won, "duel_success"] = False
    df_events.loc[selector_duel & ~selector_duel_won, "duel_failure"] = True

    # Dribbles
    df_events.loc[selector_duel & selector_dribble, "type_primary"] = "dribble"
    df_events.loc[selector_duel & selector_take_on, "type_primary"] = "take_on"

    # add location of next action or 2 actions ahead if next action is related duel
    df_events.loc[~selector_carry & selector_duel & ~selector_related_duel_next & selector_same_team_next_1, "end_x"] = \
    next_event["location_x"]
    df_events.loc[~selector_carry & selector_duel & ~selector_related_duel_next & selector_same_team_next_1, "end_y"] = \
    next_event["location_y"]
    df_events.loc[
        ~selector_carry & selector_duel & ~selector_related_duel_next & ~selector_same_team_next_1, "end_x"] = 100 - \
                                                                                                               next_event[
                                                                                                                   "location_x"]
    df_events.loc[
        ~selector_carry & selector_duel & ~selector_related_duel_next & ~selector_same_team_next_1, "end_y"] = 100 - \
                                                                                                               next_event[
                                                                                                                   "location_y"]

    df_events.loc[~selector_carry & selector_duel & selector_related_duel_next & selector_same_team_next_2, "end_x"] = \
    next_event2["location_x"]
    df_events.loc[~selector_carry & selector_duel & selector_related_duel_next & selector_same_team_next_2, "end_y"] = \
    next_event2["location_y"]
    df_events.loc[
        ~selector_carry & selector_duel & selector_related_duel_next & ~selector_same_team_next_2, "end_x"] = 100 - \
                                                                                                              next_event2[
                                                                                                                  "location_x"]
    df_events.loc[
        ~selector_carry & selector_duel & selector_related_duel_next & ~selector_same_team_next_2, "end_y"] = 100 - \
                                                                                                              next_event2[
                                                                                                                  "location_y"]

    # Reset the index
    df_events = df_events.reset_index(drop=True)

    return df_events


# def convert_duels(df_events: pd.DataFrame) -> pd.DataFrame:
#     """Convert duel events.
#
#     Parameters
#     ----------
#     df_events : pd.DataFrame
#         Wyscout event dataframe
#     Returns
#     -------
#     pd.DataFrame
#         Wyscout event dataframe in which the duels are either removed or
#         transformed into a pass
#     """
#     # Shift events dataframe by one and two time steps
#     df_events1 = df_events.shift(-1)
#     df_events2 = df_events.shift(-2)
#
#     # Define selector for same period id
#     selector_same_period = df_events["match_period"] == df_events2["match_period"]
#
#     # Define selector for duels that are followed by an 'out of field' event
#     selector_duel_out_of_field = (
#         (df_events["type_id"] == 1)
#         & (df_events1["type_id"] == 1)
#         & (df_events2["subtype_id"] == 50)
#         & selector_same_period
#     )
#
#     # Define selectors for current time step
#     selector0_duel_won = selector_duel_out_of_field & (
#         df_events["team_id"] != df_events2["team_id"]
#     )
#     selector0_duel_won_air = selector0_duel_won & (df_events["subtype_id"] == 10)
#     selector0_duel_won_not_air = selector0_duel_won & (df_events["subtype_id"] != 10)
#
#     # Define selectors for next time step
#     selector1_duel_won = selector_duel_out_of_field & (
#         df_events1["team_id"] != df_events2["team_id"]
#     )
#     selector1_duel_won_air = selector1_duel_won & (df_events1["subtype_id"] == 10)
#     selector1_duel_won_not_air = selector1_duel_won & (df_events1["subtype_id"] != 10)
#
#     # Aggregate selectors
#     selector_duel_won = selector0_duel_won | selector1_duel_won
#     selector_duel_won_air = selector0_duel_won_air | selector1_duel_won_air
#     selector_duel_won_not_air = selector0_duel_won_not_air | selector1_duel_won_not_air
#
#     # Set types and subtypes
#     df_events.loc[selector_duel_won, "type_id"] = 8
#     df_events.loc[selector_duel_won_air, "subtype_id"] = 82
#     df_events.loc[selector_duel_won_not_air, "subtype_id"] = 85
#
#     # set end location equal to ball out of field location
#     df_events.loc[selector_duel_won, "accurate"] = False
#     df_events.loc[selector_duel_won, "not_accurate"] = True
#     df_events.loc[selector_duel_won, "end_x"] = 100 - df_events2.loc[selector_duel_won, "start_x"]
#     df_events.loc[selector_duel_won, "end_y"] = 100 - df_events2.loc[selector_duel_won, "start_y"]
#
#     # df_events.loc[selector_duel_won, 'end_x'] = df_events2.loc[selector_duel_won, 'start_x']
#     # df_events.loc[selector_duel_won, 'end_y'] = df_events2.loc[selector_duel_won, 'start_y']

# Define selector for ground attacking duels with take on
# selector_attacking_duel = df_events["subtype_id"] == 11
# selector_take_on = (df_events["take_on_left"]) | (df_events["take_on_right"])
# selector_att_duel_take_on = selector_attacking_duel & selector_take_on
#
# # Set take ons type to 0
# df_events.loc[selector_att_duel_take_on, "type_id"] = 0
#
# # Set sliding tackles type to 0
# df_events.loc[df_events["sliding_tackle"], "type_id"] = 0
#
# # Remove the remaining duels
# df_events = df_events[df_events["type_id"] != 1]
#
# # Reset the index
# df_events = df_events.reset_index(drop=True)
#
# return df_events

def insert_interception_coordinates(df_events: pd.DataFrame) -> pd.DataFrame:
    """Insert interception end coordinates.
    This function adds end coordinates for interceptions as the next events starting position.
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe in which interceptions have end coordinates
    """
    # one event ahead for end coords
    next_event = df_events.shift(-1)

    # interception selector
    selector_interception = (df_events["type_primary"] == "interception")
    # same team selector
    selector_same_team_next = (df_events["team_id"] == next_event["team_id"])

    df_events.loc[selector_interception & selector_same_team_next, "end_x"] = next_event["start_x"]
    df_events.loc[selector_interception & selector_same_team_next, "end_y"] = next_event["start_y"]
    df_events.loc[selector_interception & ~selector_same_team_next, "end_x"] = 100 - next_event["start_x"]
    df_events.loc[selector_interception & ~selector_same_team_next, "end_y"] = 100 - next_event["start_y"]

    return df_events

def insert_fairplay_coordinates(df_events: pd.DataFrame) -> pd.DataFrame:
    """Insert interception end coordinates.
    This function adds end coordinates for interceptions as the next events starting position.
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe in which interceptions have end coordinates
    """
    # one event ahead for end coords
    prev_event = df_events.shift(1)
    next_event = df_events.shift(-1)
    next_event2 = df_events.shift(-2)

    # interception selector
    selector_interruption_fairplay = (df_events["type_primary"] == "game_interruption") & (next_event["type_primary"] == "fairplay")
    # same team selector
    selector_same_team_prev = (df_events["team_id"] == prev_event["team_id"])

    df_events.loc[selector_interruption_fairplay & selector_same_team_prev, ["end_x","start_x"]] = prev_event["start_x"]
    df_events.loc[selector_interruption_fairplay & selector_same_team_prev, ["end_y","start_y"]] = prev_event["start_y"]
    df_events.loc[selector_interruption_fairplay & ~selector_same_team_prev, ["end_x","start_x"]] = 100 - prev_event["start_x"]
    df_events.loc[selector_interruption_fairplay & ~selector_same_team_prev, ["end_y","start_y"]] = 100 - prev_event["start_y"]

    # fix previous events end coordinates for this case
    selector_interruption_fairplay_next = (next_event["type_primary"] == "game_interruption") & (next_event2["type_primary"] == "fairplay")

    df_events.loc[selector_interruption_fairplay_next, "end_x"] = df_events["start_x"]
    df_events.loc[selector_interruption_fairplay_next, "end_y"] = df_events["start_y"]

    return df_events

def insert_coordinates_edge_cases(df_events: pd.DataFrame) -> pd.DataFrame:
    """Insert end coordinates for remaining edge cases. Necessary for xThreat script to run
    This function adds end coordinates for remaining xT actions.
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with end coordinates for remaining on ball xT actions
    """
    idx = df_events.index.get_indexer_for(df_events[((df_events.type_primary == 'pass')
                                                 | (df_events.type_primary == 'carry')
                                                 | (df_events.type_primary == 'cross')
                                                 | (df_events.type_primary == 'acceleration')
                                                 | (df_events.type_primary == 'dribble')
                                                 | (df_events.type_primary == 'take_on')) & (df_events.end_x.isna())].index)

    # assign starting x,y to end x,y for remaining cases where na
    df_events.iloc[idx, df_events.columns.get_indexer(['end_x'])] = df_events.iloc[
        idx, df_events.columns.get_indexer(['start_x'])]

    df_events.iloc[idx, df_events.columns.get_indexer(['end_y'])] = df_events.iloc[
        idx, df_events.columns.get_indexer(['start_y'])]

    return df_events


# def insert_interception_passes(df_events: pd.DataFrame) -> pd.DataFrame:
#     """Insert interception actions before passes.
#     This function converts passes (type_id 8) that are also interceptions
#     (tag interception) in the Wyscout event data into two separate events,
#     first an interception and then a pass.
#     Parameters
#     ----------
#     df_events : pd.DataFrame
#         Wyscout event dataframe
#     Returns
#     -------
#     pd.DataFrame
#         Wyscout event dataframe in which passes that were also denoted as
#         interceptions in the Wyscout notation are transformed into two events
#     """
#     df_events_interceptions = df_events[
#         df_events["interception"] & (df_events["type_id"] == 8)
#     ].copy()
#
#     if not df_events_interceptions.empty:
#         df_events_interceptions.loc[:, [t[1] for t in wyscout_tags]] = False
#         df_events_interceptions["interception"] = True
#         df_events_interceptions["type_id"] = 0
#         df_events_interceptions["subtype_id"] = 0
#         df_events_interceptions[["end_x", "end_y"]] = df_events_interceptions[
#             ["start_x", "start_y"]
#         ]
#
#         df_events = pd.concat([df_events_interceptions, df_events], ignore_index=True)
#         df_events = df_events.sort_values(["period_id", "milliseconds"])
#         df_events = df_events.reset_index(drop=True)
#
#     return df_events


def add_offside_variable(df_events: pd.DataFrame) -> pd.DataFrame:
    """Attach offside events to the previous action.
    This function removes the offside events in the Wyscout event data and adds
    sets offside to 1 for the previous event (if this was a passing event)
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with an extra column 'offside'
    """
    # Create a new column for the offside variable
    df_events["offside"] = 0

    # Shift events dataframe by one timestep
    df_events1 = df_events.shift(-1)

    # Select offside passes
    selector_offside = (df_events1["type_primary"] == "offside") & (df_events["type_primary"] == "pass")

    # Set variable 'offside' to 1 for all offside passes
    df_events.loc[selector_offside, "offside"] = 1

    # Remove offside events
    df_events = df_events[df_events["type_primary"] != "offside"]

    # Reset index
    df_events = df_events.reset_index(drop=True)

    return df_events


# def convert_simulations(df_events: pd.DataFrame) -> pd.DataFrame:
#     """Convert simulations to failed take-ons.
#     Parameters
#     ----------
#     df_events : pd.DataFrame
#         Wyscout event dataframe
#     Returns
#     -------
#         pd.DataFrame
#         Wyscout event dataframe in which simulation events are either
#         transformed into a failed take-on
#     """
#     prev_events = df_events.shift(1)
#
#     # Select simulations
#     selector_simulation = df_events["subtype_id"] == 25
#
#     # Select actions preceded by a failed take-on
#     selector_previous_is_failed_take_on = (prev_events["take_on_left"]) | (
#         prev_events["take_on_right"]
#     ) & prev_events["not_accurate"]
#
#     # Transform simulations not preceded by a failed take-on to a failed take-on
#     df_events.loc[selector_simulation & ~selector_previous_is_failed_take_on, "type_id"] = 0
#     df_events.loc[selector_simulation & ~selector_previous_is_failed_take_on, "subtype_id"] = 0
#     df_events.loc[selector_simulation & ~selector_previous_is_failed_take_on, "accurate"] = False
#     df_events.loc[
#         selector_simulation & ~selector_previous_is_failed_take_on, "not_accurate"
#     ] = True
#     # Set take_on_left or take_on_right to True
#     df_events.loc[
#         selector_simulation & ~selector_previous_is_failed_take_on, "take_on_left"
#     ] = True
#
#     # Remove simulation events which are preceded by a failed take-on
#     df_events = df_events[~(selector_simulation & selector_previous_is_failed_take_on)]
#
#     # Reset index
#     df_events = df_events.reset_index(drop=True)
#
#     return df_events


def convert_touches(df_events: pd.DataFrame) -> pd.DataFrame:
    """touch events success/fail.
    This function converts the Wyscout 'touch' event to success or fail based on next actions
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with touch_success/touch_fail columns
    """
    df_events1 = df_events.shift(-1)

    # Carry or Touch
    selector_touch = df_events["type_primary"] == "touch"
    selector_carry = (df_events["type_carry"] == 1)

    # next action type
    selector_next_action = df_events1["type_primary"].isin(["pass", "shot",
                                                            "acceleration",
                                                            "clearance",
                                                            "touch",
                                                            "interception"])
    selector_next_action2 = df_events1["type_primary"].isin(["game_interruption",
                                                             "infraction",
                                                             "offside",
                                                             "shot_against"])
    selector_next_duel = df_events1["type_primary"] == "duel"

    # same player/team
    # selector_same_player = df_events["player_id"] == df_events1["player_id"]
    selector_same_team = df_events["team_id"] == df_events1["team_id"]

    # selector_touch_same_player = selector_touch & selector_same_player
    selector_touch_same_team = selector_touch & selector_same_team
    selector_touch_other_team = selector_touch & ~selector_same_team

    # same_x = abs(df_events["end_x"] - df_events1["start_x"]) < min_dribble_length
    # same_y = abs(df_events["end_y"] - df_events1["start_y"]) < min_dribble_length
    # same_loc = same_x & same_y

    # next duel success
    df_events.loc[selector_touch & selector_next_duel, "touch_success"] = True
    df_events.loc[selector_touch & selector_next_duel, "touch_fail"] = False

    # same team success
    df_events.loc[selector_touch_same_team & selector_next_action, 'touch_success'] = True
    df_events.loc[selector_touch_same_team & selector_next_action, 'touch_fail'] = False
    # same team failure
    df_events.loc[selector_touch_same_team & selector_next_action2, "touch_success"] = False
    df_events.loc[selector_touch_same_team & selector_next_action2, "touch_fail"] = True

    # other team success
    df_events.loc[selector_touch_other_team & selector_next_action2, "touch_success"] = True
    df_events.loc[selector_touch_other_team & selector_next_action2, "touch_fail"] = False
    # other team failure
    df_events.loc[selector_touch_other_team & selector_next_action, "touch_success"] = False
    df_events.loc[selector_touch_other_team & selector_next_action, "touch_fail"] = True

    #assign end coords
    df_events.loc[~selector_carry & selector_touch_same_team, "end_x"] = df_events1["location_x"]
    df_events.loc[~selector_carry & selector_touch_same_team, "end_y"] = df_events1["location_y"]
    # other team end coords flipped
    df_events.loc[~selector_carry & selector_touch_other_team, "end_x"] = 100 - df_events1["location_x"]
    df_events.loc[~selector_carry & selector_touch_other_team, "end_y"] = 100 - df_events1["location_y"]


    return df_events


def convert_accelerations(df_events: pd.DataFrame) -> pd.DataFrame:
    """acceleration events success/fail.
    This function converts the Wyscout 'acceleration' event to success or fail based on next actions
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        Wyscout event dataframe with acceleration_success/acceleration_fail columns
    """
    df_events1 = df_events.shift(-1)

    # Carry or acceleration
    selector_acceleration = df_events["type_primary"] == "acceleration"
    selector_carry = (df_events["type_carry"] == 1)

    # next action type
    selector_next_action = df_events1["type_primary"].isin(["pass", "shot", "acceleration",
                                                            "clearance", "acceleration", "interception"])
    selector_next_action2 = df_events1["type_primary"].isin(["game_interruption", "infraction"
                                                                                  "offside", "shot_against"])
    selector_next_duel = df_events1["type_primary"] == "duel"

    # same player/team
    # selector_same_player = df_events["player_id"] == df_events1["player_id"]
    selector_same_team = df_events["team_id"] == df_events1["team_id"]

    # selector_acceleration_same_player = selector_acceleration & selector_same_player
    selector_acceleration_same_team = selector_acceleration & selector_same_team
    selector_acceleration_other_team = selector_acceleration & ~selector_same_team

    # same_x = abs(df_events["end_x"] - df_events1["start_x"]) < min_dribble_length
    # same_y = abs(df_events["end_y"] - df_events1["start_y"]) < min_dribble_length
    # same_loc = same_x & same_y

    # next duel success
    df_events.loc[selector_acceleration & selector_next_duel, "acceleration_success"] = True
    df_events.loc[selector_acceleration & selector_next_duel, "acceleration_fail"] = False

    # same team success
    df_events.loc[selector_acceleration_same_team & selector_next_action, 'acceleration_success'] = True
    df_events.loc[selector_acceleration_same_team & selector_next_action, 'acceleration_fail'] = False
    # same team failure
    df_events.loc[selector_acceleration_same_team & selector_next_action2, "acceleration_success"] = False
    df_events.loc[selector_acceleration_same_team & selector_next_action2, "acceleration_fail"] = True

    # other team success
    df_events.loc[selector_acceleration_other_team & selector_next_action2, "acceleration_success"] = True
    df_events.loc[selector_acceleration_other_team & selector_next_action2, "acceleration_fail"] = False
    # other team failure
    df_events.loc[selector_acceleration_other_team & selector_next_action, "acceleration_success"] = False
    df_events.loc[selector_acceleration_other_team & selector_next_action, "acceleration_fail"] = True

    df_events.loc[~selector_carry & selector_acceleration_same_team, "end_x"] = df_events1["location_x"]
    df_events.loc[~selector_carry & selector_acceleration_same_team, "end_y"] = df_events1["location_y"]
    # other team end coords flipped
    df_events.loc[~selector_carry & selector_acceleration_other_team, "end_x"] = 100 - df_events1["location_x"]
    df_events.loc[~selector_carry & selector_acceleration_other_team, "end_y"] = 100 - df_events1["location_y"]


    return df_events


def create_df_actions(df_events: pd.DataFrame) -> pd.DataFrame:
    """Create the SciSports action dataframe.
    Parameters
    ----------
    df_events : pd.DataFrame
        Wyscout event dataframe
    Returns
    -------
    pd.DataFrame
        SciSports action dataframe
    """
    # df_events["time_seconds"] = df_events["milliseconds"] / 1000
    df_actions = df_events
    df_actions["original_event_id"] = df_events["id"].astype(object)
    df_actions["bodypart"] = df_events.apply(determine_bodypart_id, axis=1)
    df_actions["type_primary"] = df_events.apply(determine_type_id, axis=1)
    df_actions["result"] = df_events.apply(determine_result_id, axis=1)

    # df_actions = remove_non_actions(df_actions)  # remove all non-actions left

    return df_actions


def determine_bodypart_id(event: pd.DataFrame) -> int:
    """Determint eht body part for each action.
    Parameters
    ----------
    event : pd.Series
        Wyscout event Series
    Returns
    -------
    int
        id of the body part used for the action
    """
    if event["type_save"] == 1 or event["type_primary"] == "throw_in" or event["type_hand_pass"] == 1 \
            or event["infraction_type"] == "hand_foul":
        body_part = "other"
    # if event["subtype_id"] in [81, 36, 21, 90, 91]:
    #     body_part = "other"
    elif event["type_head_pass"] == 1 or event["type_head_shot"] or event["type_aerial_duel"] == 1:
        body_part = "head"
    else:  # all other cases
        body_part = "foot"
    return body_part


def determine_type_id(event: pd.DataFrame) -> int:  # noqa: C901
    """Determine the type of each action.
    This function transforms the Wyscout events, sub_events and tags
    into the corresponding SciSports action type
    Parameters
    ----------
    event : pd.Series
        A series from the Wyscout event dataframe
    Returns
    -------
    int
        id of the action type
    """
    # if event["own_goal"]:
    #     action_type = "bad_touch"
    if event["type_primary"] == "pass":
        if event["type_cross"] == 1:
            action_type = "cross"
        else:
            action_type = "pass"
    elif event["type_primary"] == "throw_in":
        action_type = "throw_in"
    elif event["type_primary"] == "corner":
        if event["pass_length"] > 25:
            action_type = "corner_crossed"
        else:
            action_type = "corner_short"
    elif event["type_primary"] == "free_kick":
        if event["type_free_kick_cross"] == 1:
            action_type = "free_kick_crossed"
        elif event["type_free_kick_shot"] == 1:
            action_type = "free_kick_shot"
        else:
            action_type = "free_kick_pass"
    # elif event["type_primary"] == "goal_kick":
    #     action_type = "goal_kick"
    elif event["type_primary"] == "infraction" and (event["infraction_type"] in ["hand_foul", "regular_foul"]):
        action_type = "foul"
    # elif event["type_primary"] == "shot":
    #     action_type = "shot"
    elif event["type_primary"] == "penalty":
        action_type = "shot_penalty"
    elif event["type_save"] == 1:
        action_type = "keeper_save"
    # elif event["type_primary"] == "clearance":
    #     action_type = "clearance"
    elif event["type_primary"] == "touch" and event["type_carry"] == 1:
        action_type = "carry"
    # elif event["type_primary"] == "acceleration":
    #     action_type = "dribble"
    # HANDLING DUELS??
    # elif event["take_on_left"] or event["take_on_right"]:
    #     action_type = "take_on"
    # elif event["sliding_tackle"]:
    #     action_type = "tackle"
    elif event["type_primary"] == "interception":
        action_type = "interception"
    else:
        action_type = event["type_primary"]
    # !!! Need to change below
    return action_type
    # return spadlconfig.actiontypes.index(action_type)


def determine_result_id(event: pd.DataFrame) -> int:  # noqa: C901
    """Determine the result of each event.
    Parameters
    ----------
    event : pd.Series
        Wyscout event Series
    Returns
    -------
    int
        result of the action
    """
    if event["offside"] == 1:
        return 2
    if event["type_primary"] == "foul":  # foul
        return 1
    # if event["own_goal"]:  # own goal
    #     return 3
    if event["touch_success"] == True:
        return 1
    if event["touch_fail"] == True:
        return 0
    if event["acceleration_success"] == True:
        return 1
    if event["acceleration_fail"] == True:
        return 0
    if event["shot_is_goal"] == 1:  # goal
        return 1
    if event["duel_success"] == True:
        return 1
    if event["duel_failure"] == True:
        return 0
    if event["type_primary"] in ["shot", "free_kick_shot", "shot_penalty"]:  # no goal, so 0
        return 0
    if event["type_primary"] in ["pass", "throw_in", "goal_kick",
                                 "free_kick_pass", "free_kick_crossed",
                                 "corner"]:
        if event["pass_accurate"] == 1:
            return 1
        if event["pass_accurate"] == 0:
            return 0
    if (event["type_primary"] in ["clearance", "interception"]):  # interception or clearance always success
        return 1
    if event["type_primary"] == "keeper_save":  # keeper save always success
        return 1
    # no idea, assume it was successful
    return 1


# def remove_non_actions(df_actions: pd.DataFrame) -> pd.DataFrame:
#     """Remove the remaining non_actions from the action dataframe.
#     Parameters
#     ----------
#     df_actions : pd.DataFrame
#         SciSports action dataframe
#     Returns
#     -------
#     pd.DataFrame
#         SciSports action dataframe without non-actions
#     """
#     df_actions = df_actions[df_actions["type_id"] != spadlconfig.actiontypes.index("non_action")]
#     # remove remaining ball out of field, whistle and goalkeeper from line
#     df_actions = df_actions.reset_index(drop=True)
#     return df_actions


def fix_actions(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Fix the generated actions.
    Parameters
    ----------
    df_actions : pd.DataFrame
        SPADL actions dataframe
    Returns
    -------
    pd.DataFrame
        SpADL actions dataframe with end coordinates for shots
    """
    df_actions["start_x"] = (df_actions["start_x"] * spadlconfig.field_length / 100).clip(
        0, spadlconfig.field_length
    )
    df_actions["start_y"] = (
            (100 - df_actions["start_y"])
            * spadlconfig.field_width
            / 100
        # y is from top to bottom in Wyscout
    ).clip(0, spadlconfig.field_width)
    df_actions["end_x"] = (df_actions["end_x"] * spadlconfig.field_length / 100).clip(
        0, spadlconfig.field_length
    )
    df_actions["end_y"] = (
            (100 - df_actions["end_y"])
            * spadlconfig.field_width
            / 100
        # y is from top to bottom in Wyscout
    ).clip(0, spadlconfig.field_width)
    # df_actions = fix_goalkick_coordinates(df_actions)
    # df_actions = adjust_goalkick_result(df_actions)
    # df_actions = fix_foul_coordinates(df_actions)
    df_actions = fix_keeper_save_coordinates(df_actions)
    # df_actions = remove_keeper_goal_actions(df_actions)
    df_actions.reset_index(drop=True, inplace=True)

    return df_actions


# def fix_goalkick_coordinates(df_actions: pd.DataFrame) -> pd.DataFrame:
#     """Fix goalkick coordinates.
#     This function sets the goalkick start coordinates to (5,34)
#     Parameters
#     ----------
#     df_actions : pd.DataFrame
#         SciSports action dataframe with start coordinates for goalkicks in the
#         corner of the pitch
#     Returns
#     -------
#     pd.DataFrame
#         SciSports action dataframe including start coordinates for goalkicks
#     """
#     goalkicks_idx = df_actions["type_id"] == spadlconfig.actiontypes.index("goalkick")
#     df_actions.loc[goalkicks_idx, "start_x"] = 5.0
#     df_actions.loc[goalkicks_idx, "start_y"] = 34.0
#
#     return df_actions


def fix_foul_coordinates(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Fix fould coordinates.
    This function sets foul end coordinates equal to the foul start coordinates
    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe with no end coordinates for fouls
    Returns
    -------
    pd.DataFrame
        SciSports action dataframe including start coordinates for goalkicks
    """
    fouls_idx = df_actions["type_primary"] == "foul"
    df_actions.loc[fouls_idx, "end_x"] = df_actions.loc[fouls_idx, "start_x"]
    df_actions.loc[fouls_idx, "end_y"] = df_actions.loc[fouls_idx, "start_y"]

    return df_actions


def fix_keeper_save_coordinates(df_actions: pd.DataFrame) -> pd.DataFrame:
    """Fix keeper save coordinates.
    This function sets keeper_save start coordinates equal to
    keeper_save end coordinates. It also inverts the shot coordinates to the own goal.
    Parameters
    ----------
    df_actions : pd.DataFrame
        SciSports action dataframe with start coordinates in the corner of the pitch
    Returns
    -------
    pd.DataFrame
        SciSports action dataframe with correct keeper_save coordinates
    """
    saves_idx = df_actions["type_primary"] == "keeper_save"
    # invert the coordinates
    df_actions.loc[saves_idx, "end_x"] = (
            spadlconfig.field_length - df_actions.loc[saves_idx, "end_x"]
    )
    df_actions.loc[saves_idx, "end_y"] = (
            spadlconfig.field_width - df_actions.loc[saves_idx, "end_y"]
    )
    # set start coordinates equal to start coordinates
    df_actions.loc[saves_idx, "start_x"] = df_actions.loc[saves_idx, "end_x"]
    df_actions.loc[saves_idx, "start_y"] = df_actions.loc[saves_idx, "end_y"]

    return df_actions

# def remove_keeper_goal_actions(df_actions: pd.DataFrame) -> pd.DataFrame:
#     """Remove keeper goal-saving actions.
#     This function removes keeper_save actions that appear directly after a goal
#     Parameters
#     ----------
#     df_actions : pd.DataFrame
#         SciSports action dataframe with keeper actions directly after a goal
#     Returns
#     -------
#     pd.DataFrame
#         SciSports action dataframe without keeper actions directly after a goal
#     """
#     prev_actions = df_actions.shift(1)
#     same_phase = prev_actions.time_seconds + 10 > df_actions.time_seconds
#     shot_goals = (prev_actions.type_id == spadlconfig.actiontypes.index("shot")) & (
#         prev_actions.result_id == 1
#     )
#     penalty_goals = (prev_actions.type_id == spadlconfig.actiontypes.index("shot_penalty")) & (
#         prev_actions.result_id == 1
#     )
#     freekick_goals = (prev_actions.type_id == spadlconfig.actiontypes.index("shot_freekick")) & (
#         prev_actions.result_id == 1
#     )
#     goals = shot_goals | penalty_goals | freekick_goals
#     keeper_save = df_actions["type_id"] == spadlconfig.actiontypes.index("keeper_save")
#     goals_keepers_idx = same_phase & goals & keeper_save
#     df_actions = df_actions.drop(df_actions.index[goals_keepers_idx])
#     df_actions = df_actions.reset_index(drop=True)
#
#     return df_actions


# def adjust_goalkick_result(df_actions: pd.DataFrame) -> pd.DataFrame:
#     """Adjust goalkick results.
#     This function adjusts goalkick results depending on whether
#     the next action is performed by the same team or not
#     Parameters
#     ----------
#     df_actions : pd.DataFrame
#         SciSports action dataframe with incorrect goalkick results
#     Returns
#     -------
#     pd.DataFrame
#         SciSports action dataframe with correct goalkick results
#     """
#     nex_actions = df_actions.shift(-1)
#     goalkicks = df_actions["type_id"] == spadlconfig.actiontypes.index("goalkick")
#     same_team = df_actions["team_id"] == nex_actions["team_id"]
#     accurate = same_team & goalkicks
#     not_accurate = ~same_team & goalkicks
#     df_actions.loc[accurate, "result_id"] = 1
#     df_actions.loc[not_accurate, "result_id"] = 0
#
#     return df_actions
#
