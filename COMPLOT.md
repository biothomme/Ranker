# Discussions and decisions
Here you can find the timeline of the project.

---
## 10.05.2021 - Intro
First meeting. Introduction between protagonists and with metagenomics. Decision on making a literature review about metagenomics, machine learning and remote sensing.

---
## 03.06.2021 - Literature review
Presentation about metagenomics, machine learning and remote sensing (Links: [Recording](https://www.dropbox.com/s/upav2m02r4y2pko/2021-06-02-Meeting-Thomas-Huber.mp4?dl=0), [References and slides](https://drive.google.com/drive/folders/1fI2ZYV6JWod_JK2vWj8XdwSpzPeF_XeK?usp=sharing)). We decided to try to combine metagenomics and earth image data for predicting land use remotely. The idea came from a paper by Mendes, et al. 2015, who shows that soil microbiome is dependent on land use of the area.
Next steps should explore metagenome databases, the dataset of a machine learning project on biodiversity (GeoLifeClef) and the methods in the Mendes2015 paper.

Additionally, a Google Drive for all the data and files was established [here](https://drive.google.com/drive/folders/15XHUK66QSYqCRqnQ0RJEmd3JkxFye84b?usp=sharing)
We started a Slack channel and a [github repo](https://github.com/biothomme/Ranker.git) to share codes.

---
## 03.07.2021 - Possible datasets

We discussed (i) the datasets/databases for soil metagenomes (EBI, MG-RAST, EMP), (ii) the GeoLifeClef (GLC) dataset and (iii) methods of Mendes2015 using some jupyter notebooks (respectively: (i) [soil microbiome databases](dbexploration/map_soil_databases.ipynb), (ii) [exploring the GLC dataset](dbexploration/what_about_geolifeclef.ipynb), (iii) [analysis of Mendes2015 methods](metagenomethodo/how_to_compare_the_metagenomes.ipynb)).

Decision on implementing a tool to download data for a given location (coordinates). Therefore databases for earth images, soil and climate properties, etc. should be used. This can later be of advantage to assemble different kind of data corresponding to our available metagenome locations. 

**Tasks to implement**
    [x] Initialize module in python
    [x] Design abstract mining class (input location, output (Image|value|list))
    [ ] Build dummy mining class with csv output
    [ ] Build mining class for NAIP
    [ ] Apply stitching to NAIP images
    [ ] Collect other databases
    [ ] Implement listing of data collection for mining multiple databases at once
    [ ] Build mining classes for other databases
    [ ] Construct jupy NB to test mining
    [ ] Connect with GeoLifeClef dataset

<details>
  <summary>Chat protocoll</summary>
    18:56:12 From Lucas Czech to Everyone : https://mycokeys.pensoft.net/article/20887/<br>
    19:06:40 From thomas huber to Everyone : https://www.ebi.ac.uk/ena/browser/view/PRJEB6596?show=reads<br>
    19:09:49 From David Dao to Everyone : http://lila.science/datasets/chesapeakelandcover<br>
    19:28:15 From David Dao to Everyone : ViT<br>
    19:28:32 From David Dao to Everyone : Attention<br>
    19:29:54 From David Dao to Everyone : 1,2,3,4,5<br>
    19:30:01 From David Dao to Everyone : pixel = [1,2,3,4,5]<br>
    19:30:10 From David Dao to Everyone : genom_true = [0,1,0,0,0]<br>
    19:30:21 From David Dao to Everyone : genom_data = [EBI-124]<br>
    19:30:37 From David Dao to Everyone : genom_true = [0,1,0,0,1]<br>
    19:30:56 From David Dao to Everyone : genom_data = [EBI-1, EBI-2]<br>
    19:33:18 From David Dao to Everyone : genom_true, genom_data<br>
    19:36:27 From David Dao to Everyone : D = (ebi_id, gps_coord, rgb_patch, … )<br>
    19:37:31 From David Dao to Everyone : 1. Schritt<br>
    19:37:48 From David Dao to Everyone : for i in EBI:<br>
    19:38:20 From David Dao to Everyone : d = getGeoCLEFData(i)<br>
    19:38:29 From David Dao to Everyone : return (d, i, gps)<br>
    19:39:47 From David Dao to Everyone : d_id = GetGeoCLEFDataID(i)<br>
    19:39:52 From David Dao to Everyone : (d_id, i)<br>
    19:45:37 From David Dao to Everyone : GetNAIPTiles(i)<br>
    20:09:13 From David Dao to Everyone : https://developers.google.com/earth-engine/datasets/catalog/<br>
    20:09:34 From David Dao to Everyone : https://developers.google.com/earth-engine/datasets/catalog/USDA_NAIP_DOQQ<br>
    20:11:02 From David Dao to Everyone : https://planetarycomputer.microsoft.com/catalog<br>
</details>

Next meeting: 17.07.2021

---