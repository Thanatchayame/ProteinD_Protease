import requests
import pandas as pd
import time
import os

# Configuration
base_url   = "https://dbaasp.org"
batch_size = 1000
csv_path   = "peptide_activities.csv"

# Desired column order (including SynthesisType)
SCHEMA = [
    "Peptide_ID", "Monomer_Name", "sequence", "sequenceLength",
    "Target_Species", "Measure_Group", "Measure_Value",
    "Concentration", "Unit", "Medium", "CFU",
    "Target_Groups", "Target_Objects", "SynthesisType"
]

# Clean slate
if os.path.exists(csv_path):
    os.remove(csv_path)

file_exists   = False
batch_records = []
max_n = 8001

for n in range(8000,8001):
    peptide_id = f"DBAASPR_{n}"
    endpoint   = f"/peptides/{peptide_id}"

    print(f"Processing {peptide_id}…")
    try:
        resp = requests.get(base_url + endpoint)
        time.sleep(0.5)
        peptide_data = resp.json() if resp.status_code == 200 else {}

        # — pull synthesisType.name (there is no “value” field) —
        synth_value = peptide_data.get("synthesisType", {}).get("name", "")

        # — Extract groups/objects safely —
        groups       = peptide_data.get("targetGroups")  or []
        objects      = peptide_data.get("targetObjects") or []
        group_names  = ";".join(g.get("name","") for g in groups)
        object_names = ";".join(o.get("name","") for o in objects)

        if resp.status_code == 200:

            # —— monomers branch ——
            if peptide_data.get("monomers"):
                for monomer in peptide_data["monomers"]:
                    seq     = monomer.get("sequence","")
                    seq_len = monomer.get("sequenceLength","")
                    mon_name= monomer.get("name","Unknown")

                    for activity in monomer.get("targetActivities", []):
                        if "activityMeasureGroup" not in activity:
                            continue

                        unit   = activity.get("unit")   or {}
                        medium = activity.get("medium") or {}

                        batch_records.append({
                            "Peptide_ID":     peptide_id,
                            "Monomer_Name":   mon_name,
                            "sequence":       seq,
                            "sequenceLength": seq_len,
                            "Target_Species": activity.get("targetSpecies",{}).get("name",""),
                            "Measure_Group":  activity["activityMeasureGroup"].get("name",""),
                            "Measure_Value":  activity.get("activityMeasureValue",""),
                            "Concentration":  activity.get("concentration",""),
                            "Unit":           unit.get("name",""),
                            "Medium":         medium.get("name",""),
                            "CFU":            activity.get("cfu",""),
                            "Target_Groups":  group_names,
                            "Target_Objects": object_names,
                            "SynthesisType":   synth_value
                        })

            # —— top-level targetActivities branch ——
            elif peptide_data.get("targetActivities"):
                seq     = peptide_data.get("sequence","")
                seq_len = peptide_data.get("sequenceLength","")
                for activity in peptide_data["targetActivities"]:
                    if "activityMeasureGroup" not in activity:
                        continue

                    unit   = activity.get("unit")   or {}
                    medium = activity.get("medium") or {}

                    batch_records.append({
                        "Peptide_ID":     peptide_id,
                        "Monomer_Name":   "N/A",
                        "sequence":       seq,
                        "sequenceLength": seq_len,
                        "Target_Species": activity.get("targetSpecies",{}).get("name",""),
                        "Measure_Group":  activity["activityMeasureGroup"].get("name",""),
                        "Measure_Value":  activity.get("activityMeasureValue",""),
                        "Concentration":  activity.get("concentration",""),
                        "Unit":           unit.get("name",""),
                        "Medium":         medium.get("name",""),
                        "CFU":            activity.get("cfu",""),
                        "Target_Groups":  group_names,
                        "Target_Objects": object_names,
                        "SynthesisType":   synth_value
                    })

            # —— no activities found ——
            else:
                batch_records.append({
                    "Peptide_ID":     peptide_id,
                    "Monomer_Name":   "N/A",
                    "sequence":       peptide_data.get("sequence",""),
                    "sequenceLength": peptide_data.get("sequenceLength",""),
                    "Target_Species": "N/A",
                    "Measure_Group":  "N/A",
                    "Measure_Value":  "N/A",
                    "Concentration":  "N/A",
                    "Unit":           "N/A",
                    "Medium":         "N/A",
                    "CFU":            "N/A",
                    "Target_Groups":  group_names,
                    "Target_Objects": object_names,
                    "SynthesisType":   synth_value
                })

        else:
            print(f"  → HTTP {resp.status_code}")
            batch_records.append({
                "Peptide_ID":     peptide_id,
                "Monomer_Name":   "Request Failed",
                "sequence":       "",
                "sequenceLength": "",
                "Target_Species": f"Status: {resp.status_code}",
                "Measure_Group":  "N/A",
                "Measure_Value":  "N/A",
                "Concentration":  "N/A",
                "Unit":           "N/A",
                "Medium":         "N/A",
                "CFU":            "N/A",
                "Target_Groups":  group_names,
                "Target_Objects": object_names,
                "SynthesisType":   synth_value
            })

    except Exception as e:
        print(f"  → Exception: {e}")
        batch_records.append({
            "Peptide_ID":     peptide_id,
            "Monomer_Name":   "Exception",
            "sequence":       "",
            "sequenceLength": "",
            "Target_Species": str(e),
            "Measure_Group":  "N/A",
            "Measure_Value":  "N/A",
            "Concentration":  "N/A",
            "Unit":           "N/A",
            "Medium":         "N/A",
            "CFU":            "N/A",
            "Target_Groups":  group_names,
            "Target_Objects": object_names,
            "SynthesisType":   synth_value
        })

    # —— flush every batch_size or at the very end ——
    if (n % batch_size == 0) or (n == max_n):
        df_batch = pd.DataFrame(batch_records)
        df_batch = df_batch.reindex(columns=SCHEMA)
        df_batch.to_csv(
            csv_path,
            mode='a',
            header=not file_exists,
            index=False
        )
        file_exists = True
        batch_records.clear()

print("All done — results written to", csv_path)
