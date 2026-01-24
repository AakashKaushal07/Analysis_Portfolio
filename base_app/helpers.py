from .models import ConfigItems
from leagues.models import Competition
from .TEAM_MAP import  TEAM_MAPPING
from rapidfuzz import fuzz
from unidecode import unidecode
import re
import sys,os,traceback,linecache

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

# Internal: normalize a team name (accents, lowercase, separators → spaces)
def _normalize_team(name: str) -> str:
    name = unidecode(name).lower().strip()
    name = re.sub(r'[^a-z0-9]+', ' ', name)
    return name

# Internal: compute acronym from team name
def _acronym(name: str) -> str:
    tokens = name.split()
    return "".join(t[0] for t in tokens if t)

# Public function
def best_fuzzy_match(team_name, choices, threshold_single=90, threshold_multi=75):
    q_norm = _normalize_team(team_name)
    q_tokens = q_norm.split()
    single_word = len(q_tokens) == 1
    q_acr = _acronym(q_norm)

    # Normalize mapping bidirectionally
    normalized_mapping = {}
    for k, v in TEAM_MAPPING.items():
        # k_norm = _normalize_team(k)
        # v_norm = _normalize_team(v)
        # normalized_mapping[k_norm] = v_norm
        # normalized_mapping[v_norm] = k_norm  # bidirectional
        k_norm = _normalize_team(k)
        # v_norm = [_normalize_team(v)
        normalized_mapping[k_norm] = [_normalize_team(x) for x in v]
        # normalized_mapping[v_norm] = k_norm  # bidirectional
        for x in v :
            normalized_mapping[_normalize_team(x)] = k_norm
    
    results = {}
    for choice in choices:
        c_norm = _normalize_team(choice)
        c_acr = _acronym(c_norm)
        score = 0
        # Mapping check (bidirectional)
        for map_key, map_val in normalized_mapping.items():
            if (q_norm == map_key and c_norm == map_val) or (q_norm == map_val and c_norm == map_key):
                score = 101
                break
            if c_norm in normalized_mapping.get(q_norm,[]):
                score=102
                break
        # Regular fuzzy scoring if mapping didn't match
        if score == 0:
            ts = fuzz.token_set_ratio(q_norm, c_norm)
            tsort = fuzz.token_sort_ratio(q_norm, c_norm)
            pr = fuzz.partial_ratio(q_norm, c_norm)
            score = max(ts, tsort, pr)

            # Acronym logic
            acr_score = 0
            if len(q_norm) <= 4 and q_norm.isalpha():
                if q_norm == c_acr:
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
    # print("results : ",results)
    # print(team_name,"score : ",score)
    
    return dict(sorted(results.items(), key=lambda x: x[1], reverse=True))

def log_exception(e: Exception, logger=None, full_traceback=True):
    """
    Logs or prints detailed exception info, optionally with full traceback.

    Parameters:
        e               : Exception instance
        logger          : Optional logger object (must have .error method)
        full_traceback  : Whether to include full traceback
    """
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    lineno = exc_tb.tb_lineno
    code_line = linecache.getline(exc_tb.tb_frame.f_code.co_filename, lineno).strip()

    # Build the message
    msg = (
        "\n" + "-"*50 + "\n"
        f"Exception   : {e}\n"
        f"Type        : {exc_type.__name__}\n"
        f"File        : {fname}\n"
        f"Line No     : {lineno}\n"
        f"Code        : {code_line}\n"
    )

    if full_traceback:
        tb_str = ''.join(traceback.format_exception(exc_type, exc_obj, exc_tb))
        msg += f"Full Traceback:\n{tb_str}"

    msg += "-"*50

    # Log or print
    if logger and hasattr(logger, 'error'):
        logger.error(msg)
    else:
        print(msg)

__all__ = ["best_fuzzy_match","get_name_mappings","fetch_configurations","log_exception"]