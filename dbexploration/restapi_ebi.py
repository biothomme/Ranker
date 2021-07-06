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
def run(file_name):
    """Download the metadata for EBI metagenomics soil samples."""
#    if (os.path.exists(file_name)):
#        print(f"File {file_name} already exists. Nothing was downloaded.")
#        return
    
    sysprint = sys.stdout
    print("Starting to download EBI metadata...")
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
            i = 0
            for db in DATABASES:
                for sample in session.iterate(db, api_filter):
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
                    i += 1

            print(f"Data retrieved from the API.\nOverall found samples: {i}.")
    return

def run_on_mgrast(file_name):
    """Download the metadata for MG-RAST soil samples."""
    # more information about the API: https://api.mg-rast.org/api.html
    from urllib.request import Request, urlopen
    import json
    MG_RAST_API_BASE = "https://api.mg-rast.org/metagenome?"
    LIMITS = 1000 # set to 1000 max by MG-RAST
    ENV_PACKAGE = "soil"
    CENTRAL_QUERY = "&order=id&direction=asc&match=all&status=both&verbosity=mixs&offset="
    with open(file_name, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        
        print("Starting to download MG-RAST metadata...")

        req = Request(f"{MG_RAST_API_BASE}limit={LIMITS}&env_package={ENV_PACKAGE}&{CENTRAL_QUERY}0")
        i = 0
        while(True):
            result = urlopen(req)
            content = result.read().decode("utf8")
            obj = json.loads(content)
            for sample in obj["data"]:
                i += 1
                row = dict(zip(FIELDNAMES,
                    [sample["id"],
                     sample["name"],
                     sample["longitude"],
                     sample["latitude"],
                     sample["country"],
                     sample["sequence_type"], # here we place sequence_type instead of studies on purpose!!!
                     sample["biome"],
                     sample["feature"],
                     sample["material"]]))
                writer.writerow(row)
            try:
                req = Request(obj["next"])
            except ValueError:
                print(f"Data retrieved from the API.\nOverall found samples: {i}.")
                return

