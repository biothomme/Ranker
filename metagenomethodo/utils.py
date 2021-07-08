# key functionalities for the notebook how_to_compare_the_metagenomes.ipynb

import os
import pandas

def map_amazon():
    """Plot the distribution of the samples of the amzon study on sattelite image."""
    import geopandas as gpd
    import folium
    FOL_COLS = ['green', 'cadetblue', 'gray', 'red']
    ENVIRONMENTS = ["Forest", "Deforested", "Agriculture", "Pasture"]
    LOCATIONS = [[ -11.68194, -55.83578], [-11.68338, -55.83432], 
            [-11.68474, -55.83709], [-11.71721, -55.79486]]
    geo_dict = {ev: loc for ev, loc in zip(ENVIRONMENTS, LOCATIONS)}
    col_dict = {ev: col for ev, col in zip(ENVIRONMENTS, FOL_COLS)}
    mp = folium.Map(zoom_start=50)
    TILE = folium.TileLayer(
        tiles = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr = 'Esri',
        name = 'Esri Satellite',
        overlay = False,
        control = True
       ).add_to(mp)
    for ev in ENVIRONMENTS:
        mp.add_child(
            folium.Marker(
                location=geo_dict[ev],
                popup=ev,
                icon=folium.Icon(color=col_dict[ev])
            ))
    lowest = [min([LOCATIONS[j][i] for j in range(len(LOCATIONS))]) for i in [0, 1]] 
    highest = [max([LOCATIONS[j][i] for j in range(len(LOCATIONS))]) for i in [0, 1]]

    mp.fit_bounds([lowest, highest]) 
    return mp
