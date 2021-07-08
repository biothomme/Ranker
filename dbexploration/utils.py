#

import os
import pandas as pd
import geopandas as gpd
from datetime import date

import restapi_ebi

EMP_METADATA = "emp_qiime_mapping_release1_20170912.csv"
EBI_METADATA = "ebi_ena_soil_dataset"
MGRAST_METADATA = "mgrast_soil_dataset"

def import_gpframe(file_name, sep="\t", subsample=False):
    """ Import given csv/tsv file as geopandas geopandas dataframe. """
    SELECTED_COLS = ['#SampleID', 'BarcodeSequence', 'LinkerPrimerSequence', 'Description',
       'title', 'principal_investigator', 'doi',
       'ebi_accession', 'target_gene', 'target_subfragment', 'pcr_primers',
       'illumina_technology', 'extraction_center', 'run_center', 'run_date',
       'read_length_bp', 'sequences_split_libraries', 'qc_filtered', 'subset_10k', 'subset_5k', 'subset_2k',
       'sample_taxid', 'sample_scientific_name', 'collection_timestamp', 'country', 'latitude_deg', 'longitude_deg',
       'depth_m', 'altitude_m', 'elevation_m', 'env_biome', 'env_feature',
       'env_material', 'envo_biome_0', 'envo_biome_1', 'envo_biome_2',
       'envo_biome_3', 'envo_biome_4', 'envo_biome_5', 'empo_0', 'empo_1',
       'empo_2', 'empo_3']
    df = pd.read_csv(file_name, sep=sep)
    if "std_country" in df.columns:
        SELECTED_COLS.append("std_country")
    # we only want data with locations
    df = df.dropna(subset=["latitude_deg", "longitude_deg"])
    # we do not need all columns of the EMP dataset (e.g. about host etc.)
    if subsample: df = df.loc[:, SELECTED_COLS]
    # for later analysis gp_dataframes can be useful
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["longitude_deg"], df["latitude_deg"]))
    # standardize the column of country by using the coordinates to assign countries
    if "std_country" not in gdf.columns and "processed" not in file_name:
        gdf = reassign_country(gdf)
        gdf.to_csv(str(file_name).replace(".csv", "_processed.csv"))
    return gdf


def total_n_samples(gp_dataframe):
    """ Print total numbers of samples in dataset."""
    print(f"Overall there are {gp_dataframe.shape[0]} samples within the dataset.")
    return


def summarize_cntr_and_ftrs(gp_dataframe):
    """ Display sampling locations sorted by country with biome info """
    unq = lambda x: list(set(x)) if len(set(x)) != 1 else list(set(x))[0] # strangely the pd.unique does not work here for one frame
    if "experiment_type" in gp_dataframe.columns:
        smdf = gp_dataframe.groupby(["std_country", "latitude_deg", "longitude_deg"]).agg(
            experiment_type=pd.NamedAgg(column="experiment_type", aggfunc=unq),#, aggfunc=pd.unique),
            biome=pd.NamedAgg(column="env_biome", aggfunc=unq),
            n_samples=pd.NamedAgg(column="env_biome", aggfunc="size"))
    else:
        smdf = gp_dataframe.groupby(["std_country", "latitude_deg", "longitude_deg"]).agg(
            biome=pd.NamedAgg(column="env_biome", aggfunc=unq),
            n_samples=pd.NamedAgg(column="env_biome", aggfunc="size"))
    return smdf


def subset_for_soil():
    """ Make a subset only containing soil samples """
    global emp_sdf
    QUERY = "soil" #  "soil|rhizosphere"
    emp_sdf = emp_df[(
        emp_df["env_feature"].str.contains(QUERY, regex=True) |
        emp_df["env_material"].str.contains(QUERY, regex=True)
    )]
    emp_sdf = emp_sdf.dropna(subset=["latitude_deg", "longitude_deg"])
    return

def subset_for_coordprec(i_df):
    """ Make a subset only containing samples with precise coordinates."""
    i_df = i_df.dropna(subset=["latitude_deg", "longitude_deg"])
    return i_df[
            i_df["latitude_deg"].apply(
                filter_low_resolution) & i_df["longitude_deg"].apply(
                    filter_low_resolution)]


def subset_for_countries(i_df):
    """ Make a subset only containing samples from USA and Canada."""
    QUERY = "United States|Canada" #  "soil|rhizosphere"
    i_df = i_df[i_df["countries"].str.contains(QUERY, regex=True)]
    return


def reassign_country(gpd_df):
    """Assign country to each sample by comparing coordinates to world countries shape file."""
    country_file = os.path.join(data_dir, "TM_WORLD_BORDERS-0.3/TM_WORLD_BORDERS-0.3.shp")
    shapes = gpd.read_file(country_file)
    gpd_df["std_country"] = ["NaN"]*gpd_df.shape[0]
    for i, geom in gpd_df.geometry.items():
        for j, shape in shapes.iterrows():
            if geom.within(shape.geometry):
                gpd_df.loc[i, "std_country"] = shape.NAME
                break
    return gpd_df


def overall_env_features(gp_dataframe):
    """ Print all different environmental/sample features."""
    print("--- Biomes ---")
    print(gp_dataframe.env_biome.value_counts())
    print("\n--- Material ---")
    print(gp_dataframe.env_material.value_counts())
    print("\n--- Features ---")
    print(gp_dataframe.env_feature.value_counts())
    return


def map_the_data(gp_dataframe, experiment_type=False):
    """Plot the distribution of all datasamples on interactive map."""
    import folium
    FOL_COLS = ['darkred', 'white', 'cadetblue', 'pink', 'red', 'gray',
            'darkgreen', 'green', 'black', 'lightred', 'blue', 'beige',
            'lightgray', 'lightgreen', 'darkpurple', 'darkblue',
            'lightblue', 'purple']
    mp = folium.Map(zoom_start=1)
    locations = summarize_cntr_and_ftrs(gp_dataframe)
    col_dict = {
            biome: col for biome, col in zip(
                gp_dataframe.env_biome.unique(), FOL_COLS*10)}
    if experiment_type:
        col_dict = {
                biome: col for biome, col in zip(
                    gp_dataframe.experiment_type.unique(), FOL_COLS*10)}
    for coordinates, samples in locations.groupby(level=[1, 2]):
        bm = samples.biome[0]
        if experiment_type:
            et = samples.experiment_type[0]
        if experiment_type: label = et
        else: label = bm
        if type(label) != str:
            loc_col = "orange"
        else:
            loc_col = col_dict[label]
        popup = f"Biomes: {bm}\nSamples: {samples.n_samples[0]}"
        if experiment_type: popup = f"{popup}\nExp. type: {et}"
        mp.add_child(
            folium.Marker(
                location=coordinates,
                popup=popup,
                icon=folium.Icon(color=loc_col)
            ))
    return mp


def load_ebi_data():
    """Download the metadata from ebi metagenomics soil samples."""
    if any(EBI_METADATA in ds for ds in os.listdir(data_dir)):
        if ask_for_ebi_reload():
            restapi_ebi.run(os.path.join(data_dir, f"{EBI_METADATA}_{date.today().strftime('%Y_%m_%d')}.csv"))
    else:
        restapi_ebi.run(os.path.join(data_dir, f"{EBI_METADATA}_{date.today().strftime('%Y_%m_%d')}.csv"))
    return


def load_mg_rast_data(total=False):
    """Download the metadata from MG-RAST soil (or total) samples."""
    if total:
        mgmd_name = MGRAST_METADATA.replace("soil", "total")
    else:
        mgmd_name = MGRAST_METADATA
    if any(mgmd_name in ds for ds in os.listdir(data_dir)):
        if ask_for_ebi_reload():
            restapi_ebi.run_on_mgrast(os.path.join(data_dir, f"{mgmd_name}_{date.today().strftime('%Y_%m_%d')}.csv"), total=total)
    else:
        restapi_ebi.run_on_mgrast(os.path.join(data_dir, f"{mgmd_name}_{date.today().strftime('%Y_%m_%d')}.csv"), total=total)
    return


def ask_for_ebi_reload():
    """Get user input to decide if ENA metadata set should be reloaded."""
    CHOICES = {"y": True, "n": False}
    while(True):
        answer = input("Do you want to reload the metadata from EBI (or MG-RAST) database? [y/n]")
        for ans, choice in CHOICES.items():
            if answer.lower() == ans:
                return choice
        print(f"Please enter 'y' or 'n', not {answer}. Retry ...")
    return


def import_ebi(mg_rast=False):
    """Import and globally store ebi metadata set from latest download."""
    data_file = EBI_METADATA
    if mg_rast:
        data_file = MGRAST_METADATA
    ebi_metapostfix = sorted([ds for ds in os.listdir(data_dir) if data_file in ds])[-1]
    ebi_metafile = os.path.join(data_dir, ebi_metapostfix)
    print(ebi_metafile)
    if mg_rast:
        global mgrast_df
        mgrast_df = import_gpframe(ebi_metafile, sep=",")
        return
    global ebi_df
    ebi_df = import_gpframe(ebi_metafile, sep=",")
    return


def filter_low_resolution(coordinate, sign_figures = 3):
    """Filter dataset by resolution of coordinates."""
    return round(coordinate, sign_figures-1) - coordinate != 0


def dist_between_coord(point1, point2):
    """Illustrate distance between two coordinates by applying haversines formula."""
    from haversine import haversine
    distance = haversine(point1[0], point1[1], point2[0], point2[1])
    print("The points [", point1[1], "°N,", point1[0],
            "°E ] and [", point2[1], "°N,", point2[0], "°E ] have a distance of:")
    print(round(distance*1000, 2), "m")
    return


def main():
    pd.options.display.max_rows = 9999

    global parent_dir, data_dir
    parent_dir = os.path.dirname(os.getcwd())
    data_dir = os.path.join(parent_dir, "data")
    
    # import emp meta dataset
    if os.path.exists(os.path.join(data_dir, EMP_METADATA.replace(".csv", "_processed.csv"))):
        emp_metafile = os.path.join(data_dir, EMP_METADATA.replace(".csv", "_processed.csv"))
        sep = ","
    else:
        emp_metafile = os.path.join(data_dir, EMP_METADATA)
        sep = ","
    global emp_df
    emp_df = import_gpframe(emp_metafile, sep=sep, subsample=True)
    
    # import latest ena ebi metagenomica dataset
    if any(EBI_METADATA in ds for ds in os.listdir(data_dir)):
        ebi_metapostfix = sorted([ds for ds in os.listdir(data_dir) if EBI_METADATA in ds])[-1]
        ebi_metafile = os.path.join(data_dir, ebi_metapostfix)
        global ebi_df
        ebi_df = import_gpframe(ebi_metafile, sep=",")
    return


# RUN
main()
