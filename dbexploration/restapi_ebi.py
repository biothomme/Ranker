# Script with function to download metadata of all Soil, Rhizoplane and Rhizosphere samples on EBI metagenomics.
#
# For this script I had a lot of inspiration by: 
# https://github.com/EBI-Metagenomics/mgnify-ebi-2020/blob/master/docs/source/scripts/api/exercise1.py
import csv
import os
import sys
from urllib.parse import urlencode
from jsonapi_client import Filter, Session

API_BASE = "https://www.ebi.ac.uk/metagenomics/api/latest"
DATABASES = [
        "biomes/root:Environmental:Terrestrial:Soil/samples",
        "biomes/root:Host-associated:Plants:Rhizoplane/samples",
        "biomes/root:Host-associated:Plants:Rhizosphere/samples"]

def run(file_name):
    """Download the metadata for EBI metagenomics soil samples."""
    FIELDNAMES = [
        "accession",
        "sample_name",
        "longitude_deg",
        "latitude_deg",
        "country",
        "studies",
        "env_biome",
        "env_feature",
        "env_material"
    ]
#    if (os.path.exists(file_name)):
#        print(f"File {file_name} already exists. Nothing was downloaded.")
#        return
    
    sysprint = sys.stdout
    print("Starting to downlad EBI metadata...")
    with open(file_name, "w") as csvfile:
        # CSV initialization
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        # API call
        with Session(API_BASE) as session:

            # configure the filters
            params = {
                "ordering": "accession",
                # other filters should be placed here
            }

            api_filter = Filter(urlencode(params))

            # sessions.iterate will take care of the pagination for us
            sysprint.write(f"Downloaded samples:")
            sysprint.flush()
            for i, sample in enumerate(
                    session.iterate(f"{DATABASES[1]}/samples",
                        api_filter)):
                # if (sample.latitude != "" and sample.longitude != ""):  # avoid samples without coordinates
                    if i % 100 == 0:
                        sysprint.write(f"\t{i}")
                        sysprint.flush()
                    row = dict(zip(FIELDNAMES,
                        [sample.accession,
                         sample.sample_name,
                         sample.longitude,
                         sample.latitude,
                         sample.geo_loc_name,
                         ",".join([study.accession for study in sample.studies]),
                         sample.environment_biome,
                         sample.environment_feature,
                         sample.environment_material]))
                    writer.writerow(row)

            print(f"Data retrieved from the API.\nOverall found samples: {i}.")
    return
