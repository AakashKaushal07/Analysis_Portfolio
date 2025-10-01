from .models import ConfigItems
from leagues.models import Competition
from .TEAM_MAP import  TEAM_MAPPING
from rapidfuzz import fuzz
from unidecode import unidecode
import re

def fetch_configurations():
        config_dict = {}
        for item in ConfigItems.objects.all():
            config_dict[item.key] = item.value
        return config_dict

def get_name_mappings(source="SA",target="N",source_as_file_names=False,target_as_file_name=False,use_with_region_names=True):
    
    if source.lower() not in ["sa",'fm','n'] :
        raise Exception("Only allowed values of 'Source' are 'SA' for ScoresAway; 'FM' for FotMob and 'N' for Competetion Name.")
    if target.lower() not in ["sa",'fm','n'] :
        raise Exception("Only allowed values of 'Target' are 'SA' for ScoresAway; 'FM' for FotMob and 'N' for Competetion Name.")
    if source.lower() == target.lower() :
        raise Exception("Source and Target are same, terminating process.")
    
    key_map = {
        "sa" : "name_scoresaway","fm":"name_fotmob","n":"competition_name"
    }
    source = source.lower()
    target = target.lower()
    
    mappings = {}
    for obj in Competition.objects.all() :
        obj_dict = obj.__dict__
        src_val = obj_dict.get(key_map.get(source,'None'),'')
        tgt_val = obj_dict.get(key_map.get(target,'None'),'')
        if source_as_file_names :
            src_val = src_val.replace('.','')
        if target_as_file_name :
            tgt_val = tgt_val.replace('.','')
        if len(tgt_val)==0 or len(src_val)==0:
            print(f"Unable to find source / target value for {obj_dict}")
            continue
        if use_with_region_names :
            mappings[f"{obj.country}|{src_val}"] = tgt_val
        else:
            mappings[src_val] = tgt_val
            
    return mappings

def acronym(name: str) -> str:
    tokens = name.lower().split()
    return "".join(t[0] for t in tokens if t)

def normalize_team(name: str) -> str:
    name = unidecode(name).lower().strip()
    # Replace common non-alphanumeric separators with spaces
    name = re.sub(r'[^a-z0-9]+', ' ', name)
    return name

def best_fuzzy_match(team_name, choices, threshold_single=90, threshold_multi=75, mapping=TEAM_MAPPING):
    q = normalize_team(team_name)
    q_tokens = q.split()
    single_word = len(q_tokens) == 1
    q_acr = acronym(q)
    results = {}

    # Prepare reverse mapping for bidirectional matching
    reverse_mapping = {}
    if mapping:
        for k, v in mapping.items():
            reverse_mapping[v.lower()] = k.lower()

    for choice in choices:
        c = normalize_team(choice)
        c_acr = acronym(c)
        score = 0

        # Check mapping first (bidirectional)
        if mapping:
            if q in mapping and mapping[q].lower() == c:
                score = 100
            elif c in mapping and mapping[c].lower() == q:
                score = 100
            elif q in reverse_mapping and reverse_mapping[q] == c:
                score = 100
            elif c in reverse_mapping and reverse_mapping[c] == q:
                score = 100

        # Normal scoring if mapping didn't match
        if score == 0:
            ts = fuzz.token_set_ratio(q, c)
            tsort = fuzz.token_sort_ratio(q, c)
            pr = fuzz.partial_ratio(q, c)
            score = max(ts, tsort, pr)

            # Acronym matching logic
            acr_score = 0
            if (len(q) <= 4 and q.isalpha()):  
                if q == c_acr:
                    acr_score = 100
            else:
                if q_acr == c_acr and len(c_acr) <= 4:
                    acr_score = 100
            score = max(score, acr_score)

        # Apply thresholds
        if single_word:
            if score >= threshold_single:
                results[choice] = score
        else:
            if score >= threshold_multi:
                results[choice] = score

    return dict(sorted(results.items(), key=lambda x: x[1], reverse=True))

__all__ = ["best_fuzzy_match","get_name_mappings","fetch_configurations"]