# Tree Cover Loss Analysis

This is an ArcPy Python Toolbox which preprocesses vector data and launches
a zonal statistics analysis of GFW/GNW data on AWS EMR/Spark using ArcGIS Pro.
You can select your feature class in ArcPro. The toolbox will locally preprocess your data by
splitting features into smaller chunks for better partitioning in EMR. 
It then exports your data as a TSV file and uploads it to S3.
Then, it launches a SPARK cluster on AWS EMR and runs the zonal statistics analysis.
The last step is asynchronous; the toolbox exits while the cluster is still running.
It can take about 10-12 minutes for an EMR cluster to acquire its resource and install software before any analysis begins.

You can monitor progress directly on AWS EMR console. Final results will be stored on S3 in your user folder in
the new land-researcher AWS S3 account.

`s3://wri-lcl-users/{your.name}/geotrellis/results/treecoverloss_<date_time>/`

## Installation and dependencies

Copy or clone this repository anywhere to your filesystem.

You will need a licensed version of ArcGIS Pro to run the toolbox. 

As of August 2026, this tool has been switched over to use the Land-Researcher single sign-on (SSO) account on AWS
(as opposed to the general WRI AWS account which it used until then). 
You don't need your SSO credentials stored locally to run the tool; it is set up to prompt SSO authentication whenever needed (see below).

In ArcGIS Pro Menu navigate to the Python section and install `Boto3` package into
your virtual environment. (Note: David is not sure if this is necessary with the introduction of SSO credentials, 
but it probably is because boto3 is a required library for the tool.)

## Run

1. Open the toolbox in ArcGIS Pro and select the Tree Cover Loss Analysis tool. The tool won't open until your AWS credentials are validated (see below).
2. If ArcPro opens a popup about running third-party code, click "OK".
3. If you haven't used the tool in the last ~12 hours (the expiration duration for SSO credentials), 
the tool should open a browser window asking you to authorize the for the tool to access your SSO account. 
Press "Confirm and continue". On the next screen, click "Allow access". The browser should say "Request approved", then the tool should open.
4. Select the input feature for which you want to run the analysis.
5. Select the tree cover density threshold for which you want to compute the analysis.
You can select more than one threshold.
6. Select the tree cover density reference year you want to use: 2000 or 2010.
7. Select the forest carbon data analysis options you want to use in your analysis from the "Carbon options" collapsable menu. 
Gross emissions, gross removals, and net flux are always included in the output. 
The options in this menu allow for calculation of additional, optional outputs. 
8. Select contextual layers you want to use in your analysis from the "Contextual layers: results by..." collapsable menu. 
This will dis-aggregate results by the selected layers.
You will end up with multiple output rows per feature and tree cover density threshold.
9. You can change the number of nodes for your EMR cluster under "Spark config" collapsable menu. 
Default size is 1 master and 4 workers.

## Results

Once the analysis completes, your results will be stored on S3 (see path above).
Results are stored as a CSV file. There will be one row per feature and selected treecover density threshold and
combination of contextual layers. Input features are identified by an ID column; the output csv does not include
any other information (e.g., feature name) from your input shapefile. 

Forest carbon flux model (Harris et al. 2021 NCC, Gibbs et al. 2025 ESSD) (gross emissions, gross removals, net flux)
shown in this tool are for (TCD>X OR Hansen gain=TRUE OR mangrove presence NOT pre-2000 IDN/MYS plantations) 
because the flux model includes all Hansen gain pixels. 
In other words, the flux model results include not just pixels above the requested TCD threshold but has a few 
additional rules about which pixels are included. 
Geotrellis implements these rules beyond tree cover density without double-counting gain pixels.
The non-flux model outputs of this tool (tree cover extent, biomass, tree cover loss, carbon stocks in 2000, etc.) 
use the pure tree cover density threshold without including all gain pixels (the standard way of getting zonal statistics by TCD).
Thus, flux model results and non-flux model results are reported over slightly different sets of pixels within the 
submitted polygons and flux model results should not be divided by non-flux model results 
(e.g., do not divide gross removals by tree cover extent to get removals per hectare).
Results from the forest carbon flux model (Harris et al. 2021 NCC) (gross emissions, gross removals, net flux) 
should only be used for TCD>30 because the model is designed for forests. 

The tool will give you the object ID of the features as row identifier.

To work with results in ArcGIS Pro:
1. Download the CSV file.
2. Open it in ArcGIS Pro.
3. Optionally, apply a definition query for your tree cover density threshold if you selected more than one.
4. Join you feature class with the CSV file using the object ID.

