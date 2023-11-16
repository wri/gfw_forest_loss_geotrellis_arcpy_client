# Tree Cover Loss Analysis

This is an ArcPy Python Toolbox which preprocesses vector data and launches
GFW Tree Cover Loss Analysis on AWS EMR/Spark from within ArcMap or ArcGIS Pro.
You can select your feature class in ArcMap/ArcPro. The toolbox will locally preprocess your data by
splitting features into smaller chunks for better partitioning in EMR. 
It then exports your data as a TSV file and uploads it to S3.
Then, it launches a SPARK cluster on AWS EMR and runs the zonal statistics analysis.
The last step is asynchronous; the toolbox exits while the cluster is still running.
It can take about 10-12 minutes for an EMR cluster to acquire its resource and install software before any analysis begins.

You can monitor progress directly on AWS EMR console. Final results will be stored on S3 in your user folder.

`s3://wri-users/{your.name}/geotrellis/results/treecoverloss_<date_time>/`

## Installation and dependencies

Copy or clone this repository anywhere to your filesystem.

You will need a licensed version of ArcMap or ArcGIS Pro to run the toolbox. The toolbox itself
uses BOTO3 to communicate with AWS. You will need to install this package into your Python
installation.

Make sure, Boto3 can find your [AWS credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html) and that you have permissions to run jobs on AWS EMR. See [docs/aws.md](docs/aws.md) for details on the permissions neaded and how to get them.

__ArcMap__

1. Open the Command Line Tool with Administrator rights and run:

`C:\Python27\ArcGIS10.X\scripts\pip.exe install boto3`
Make sure you select the correct Python version which correspond with your ArcGIS version number.

2. In the ArcMap Catalog pane, add the folder gfw_forest_loss_geotrells_arcpy_client as a folder connection the way
you would with any other folder you're working with. 
The TreeCoverLossAnalysis.pyt toolbox should populate with one tool called Tree Cover Loss Analysis inside.


__ArcGIS Pro__

In ArcGIS Pro Menu navigate to the Python section and install `Boto3` package into
your virtual environment.

## Run

1. Open the toolbox in ArcMap or ArcGIS Pro and select the Tree Cover Loss Analysis tool.
2. Select the input feature for which you want to run the analysis.
3. Select the tree cover density threshold for which you want to compute the analysis.
You can select more than one threshold.
4. Select the tree cover density reference year you want to use: 2000 or 2010.
5. Select the forest carbon data analysis options you want to use in your analysis from the "Carbon options" collapsable menu. 
Gross emissions, gross removals, and net flux are always included in the output. 
The options in this menu allow for calculation of additional, optional outputs. 
6. Select contextual layers you want to use in your analysis from the "Contextual layers: results by..." collapsable menu. 
This will dis-aggregate results by the selected layers.
You will end up with multiple output rows per feature and tree cover density threshold.
7. You can change the number of nodes for your EMR cluster under "Spark config" collapsable menu. 
Default size is 1 master and 4 workers.

## Results

Once the analysis completes, your results will be stored on S3 (see path above).
Results are stored as a CSV file. There will be one row per feature and selected treecover density threshold and
combination of contextual layers. Input features are identified by an ID column; the output csv does not include
any other information (e.g., feature name) from your input shapefile. 

Forest carbon flux model (Harris et al. 2021 NCC) (gross emissions, gross removals, net flux)
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

The tool will you the object ID of the features as row identifier.

To work with results in ArcMap/ArcGIS Pro,
1. Download the CSV file
2. Open it in ArcMap/ArcGIS Pro
3. Optionally, apply a definition query for your tree cover density threshold if you selected more than one.
4. Join you feature class with the CSV file using the object ID.

