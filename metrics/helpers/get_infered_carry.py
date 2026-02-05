import numpy as np
import pandas as pd

def infer_carries_with_confidence(
    df,
    min_dist=8.5,
    max_time_gap=10
):
    TOUCH_EVENTS = {"Pass", "Ball touch", "Take On"}
    BREAK_EVENTS = {
        "Interception", "Tackle", "Dispossessed",
        "Clearance", "Out", "Foul", "Offside Pass"
    }

    df = df.copy()
    df["ts_seconds"] = df["minute"] * 60 + df["seconds"]

    carries = []
    n = len(df)

    i = 0
    while i < n:
        start = df.iloc[i]

        if (
            start["event_type"] not in TOUCH_EVENTS or
            start["outcome"] != "Successful"
        ):
            i += 1
            continue

        start_player = start["player_name"]
        start_team = start["team"]
        start_x, start_y = start["x"], start["y"]
        start_time = start["ts_seconds"]

        j = i + 1
        while j < n:
            evt = df.iloc[j]

            if (
                evt["period"] != start["period"] or
                evt["ts_seconds"] - start_time > max_time_gap or
                evt["team"] != start_team or
                evt["event_type"] in BREAK_EVENTS or
                evt["outcome"] == "Unsuccessful"
            ):
                break

            if (
                evt["player_name"] == start_player and
                evt["event_type"] in TOUCH_EVENTS and
                evt["outcome"] == "Successful"
            ):
                end_x, end_y = evt["x"], evt["y"]
                duration = evt["ts_seconds"] - start_time
                dist = np.hypot(end_x - start_x, end_y - start_y)

                if dist < min_dist:
                    break

                # --- confidence (loose) ---
                confidence = 1
                reasons = ["touch_to_touch"]

                if "Ball touch" in {start["event_type"], evt["event_type"]}:
                    confidence += 1
                    reasons.append("ball_touch")

                if "Take On" in {start["event_type"], evt["event_type"]}:
                    confidence += 2
                    reasons.append("take_on")

                if duration > 8:
                    confidence -= 1
                    reasons.append("long_duration")

                if dist > 40:
                    confidence -= 1
                    reasons.append("long_distance")

                if confidence <= 0:
                    break

                carry_type = (
                    "carry_strict" if confidence >= 2 else "carry_inferred"
                )

                carries.append({
                    "period": start["period"],
                    "team": start_team,
                    "player_name": start_player,
                    "carry_type": carry_type,
                    "carry_confidence": confidence,
                    "carry_reason": ",".join(reasons),
                    "start_x": start_x,
                    "start_y": start_y,
                    "end_x": end_x,
                    "end_y": end_y,
                    "distance": dist,
                    "duration": duration
                })
                break

            j += 1

        i += 1

    return pd.DataFrame(carries)
