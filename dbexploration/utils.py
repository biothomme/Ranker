#

import os
import pandas as pd
import geopandas as gpd

EMP_METADATA = "emp_qiime_mapping_release1_20170912.tsv"


def import_gpframe(file_name):
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
    df = pd.read_csv(file_name, sep="\t")
    # we only want data with locations
    df = df.dropna(subset=["latitude_deg", "longitude_deg"])
    # we do not need all columns (e.g. about host etc.)
    df = df.loc[:, SELECTED_COLS]
    # for later analysis gp_dataframes can be useful
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["latitude_deg"], df["longitude_deg"]))
    return gdf


def total_n_samples(gp_dataframe):
    """ Print total numbers of samples in dataset."""
    print(f"Overall there are {gp_dataframe.shape[0]} samples within the dataset.")
    return


def summarize_cntr_and_ftrs(gp_dataframe):
    """ Display sampling locations sorted by country with biome info """
    smdf = gp_dataframe.groupby(["country", "latitude_deg", "longitude_deg"]).agg(
        biome=pd.NamedAgg(column="env_biome", aggfunc=pd.unique),
        n_samples=pd.NamedAgg(column="env_biome", aggfunc="size"))
    return smdf


def subset_for_soil():
    """ Make a subset only containing soil samples """
    global sdf
    QUERY = "soil" #  "soil|rhizosphere"
    sdf = df[(
        df["env_feature"].str.contains(QUERY, regex=True) |
        df["env_material"].str.contains(QUERY, regex=True)
    )]
    sdf = sdf.dropna(subset=["latitude_deg", "longitude_deg"])
    return


def subset_for_countries():
    """ Make a subset only containing sampes from USA and Canada """
    QUERY = "United States|Canada" #  "soil|rhizosphere"
    cdf = df[df["countries"].str.contains(QUERY, regex=True)]
    return


def map_the_data(gp_dataframe):
    import folium
    FOL_COLS = ['darkred', 'white', 'cadetblue', 'pink', 'red', 'gray',
            'darkgreen', 'green', 'black', 'lightred', 'blue', 'beige',
            'lightgray', 'lightgreen', 'darkpurple', 'darkblue',
            'lightblue', 'purple']
    mp = folium.Map(zoom_start=1)
    locations = summarize_cntr_and_ftrs(gp_dataframe)
    col_dict = {
            biome: col for biome, col in zip(
                gp_dataframe.env_biome.unique(), FOL_COLS+FOL_COLS)}

    for coordinates, samples in locations.groupby(level=[1, 2]):
        bm = samples.biome[0]
        if type(bm) != str:
            loc_col = "orange"
        else:
            loc_col = col_dict[bm]
        mp.add_child(
            folium.Marker(
                location=coordinates,
                popup=f"Biomes: {bm}\nSamples: {samples.n_samples[0]}",
                icon=folium.Icon(color=loc_col)
            ))
    return mp



def main():
    pd.options.display.max_rows = 999

    parent_dir = os.path.dirname(os.getcwd())
    emp_metafile = os.path.join(parent_dir, "data", EMP_METADATA)
    global df
    df = import_gpframe(emp_metafile)    
    return


def overall_env_features(gp_dataframe):
    """ Print all different environmental/sample features """
    print("--- Biomes ---")
    print(gp_dataframe.env_biome.value_counts())
    print("\n--- Material ---")
    print(gp_dataframe.env_material.value_counts())
    print("\n--- Features ---")
    print(gp_dataframe.env_feature.value_counts())
    return

main()
