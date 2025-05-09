import requests
import pandas as pd
import time

# Base URL for the DBAASP API
base_url = "https://dbaasp.org"

# Create an empty list to store all records
all_records = []

# Loop through peptide IDs
for n in range(1, 4):  # Start from 1 and go to 100
    peptide_id = f"DBAASPR_{n}"
    endpoint = f"/peptides/{peptide_id}"
    
    print(f"Processing {peptide_id}...")
    
    # Make the GET request
    try:
        response = requests.get(base_url + endpoint)
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(0.5)
        
        # Check if request was successful
        if response.status_code == 200:
            peptide_data = response.json()
            
            # Process monomers if they exist
            if 'monomers' in peptide_data and peptide_data['monomers']:
                for monomer in peptide_data['monomers']:
                    if 'targetActivities' in monomer:
                        monomer_name = monomer.get('name', 'Unknown')
                        
                        for activity in monomer['targetActivities']:
                            if 'activityMeasureGroup' in activity:
                                # Create a record and append to our list
                                record = {
                                    'Peptide_ID': peptide_id,
                                    'Monomer_Name': monomer_name,
                                    'Target_Species': activity['targetSpecies']['name'],
                                    'Measure_Group': activity['activityMeasureGroup']['name'],
                                    'Measure_Value': activity.get('activityMeasureValue', ''),
                                    'Concentration': activity.get('concentration', ''),
                                    'Unit': activity.get('unit', {}).get('name', ''),
                                    'Medium': activity.get('medium', {}).get('name', ''),
                                    'CFU': activity.get('cfu', '')
                                }
                                all_records.append(record)
            
            # Process targetActivities directly in the peptide data
            elif 'targetActivities' in peptide_data:
                for activity in peptide_data['targetActivities']:
                    if 'activityMeasureGroup' in activity:
                        # Create a record and append to our list
                        record = {
                            'Peptide_ID': peptide_id,
                            'Monomer_Name': 'N/A',
                            'Target_Species': activity['targetSpecies']['name'],
                            'Measure_Group': activity['activityMeasureGroup']['name'],
                            'Measure_Value': activity.get('activityMeasureValue', ''),
                            'Concentration': activity.get('concentration', ''),
                            'Unit': activity.get('unit', {}).get('name', ''),
                            'Medium': activity.get('medium', {}).get('name', ''),
                            'CFU': activity.get('cfu', '')
                        }
                        all_records.append(record)
            else:
                # No activities found, still record the peptide ID
                record = {
                    'Peptide_ID': peptide_id,
                    'Monomer_Name': 'N/A',
                    'Target_Species': 'N/A',
                    'Measure_Group': 'N/A',
                    'Measure_Value': 'N/A',
                    'Concentration': 'N/A',
                    'Unit': 'N/A',
                    'Medium': 'N/A',
                    'CFU': 'N/A'
                }
                all_records.append(record)
        else:
            print(f"Error: {response.status_code} for {peptide_id}")
            
            # Record for failed request
            record = {
                'Peptide_ID': peptide_id,
                'Monomer_Name': 'Request Failed',
                'Target_Species': f'Status: {response.status_code}',
                'Measure_Group': 'N/A',
                'Measure_Value': 'N/A',
                'Concentration': 'N/A',
                'Unit': 'N/A',
                'Medium': 'N/A',
                'CFU': 'N/A'
            }
            all_records.append(record)
            
    except Exception as e:
        print(f"Exception occurred for {peptide_id}: {str(e)}")
        
        # Record for exception
        record = {
            'Peptide_ID': peptide_id,
            'Monomer_Name': 'Exception',
            'Target_Species': str(e),
            'Measure_Group': 'N/A',
            'Measure_Value': 'N/A',
            'Concentration': 'N/A',
            'Unit': 'N/A',
            'Medium': 'N/A',
            'CFU': 'N/A'
        }
        all_records.append(record)

# Create DataFrame from all records
df = pd.DataFrame(all_records)

# Save DataFrame to CSV
df.to_csv('peptide_activities.csv', index=False)

# Display summary information
print(f"Total records collected: {len(df)}")
print(f"Unique peptide IDs: {df['Peptide_ID'].nunique()}")
print(f"Measure groups found: {df['Measure_Group'].unique()}")
print("CSV file 'peptide_activities.csv' created successfully.")

# Optional: show a preview of the data
print("\nPreview of the data:")
print(df.head())
