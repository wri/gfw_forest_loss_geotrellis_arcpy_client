# Forest Loss Analysis

This is an ArcPy Python Toolbox which preprocesses vector data and launches
GFW Tree Cover Loss Analysis on AWS EMR/ Spark from within ArcMap or ArcGIS Pro.
You can select your feature class in ArcMap/ ArcPro. The toolbox will preprocess your data,
and split features into smaller chunks for better partitioning. It then export your data
as TSV file and uploads it to S3.
Afterwards it launches a SPARK cluster on AWS EMR and runs the GFW tree cover analysis.
The last step is asynchronous. The toolbox exits while the cluster is still running.

You can monitor progress directly on AWS EMR console. Final results will be stored on S3 in your user folder.

`s3://wri-users/{your user name}/geotrellis/results/treecoverloss_<date_time>/`

## Installation and dependencies

Copy or clone this repository anywhere to your filesystem.

You will need a licensed version of ArcMap or ArcGIS Pro to run the toolbox. The toolbox itself
uses BOTO3 to communicate with AWS. You will need to install this package into your Python
installation.

Make sure, Boto3 can find your [AWS credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html) and that you have permissions to run jobs on AWS EMR.

__ArcMap__

Open the Command Line Tool with Administrator rights and run:

`C:\Python27\ArcGIS10.X\scripts\pip.exe install boto3`
Make sure you select the correct Python version which correspond with your ArcGIS version number.


__ArcGIS Pro__

In ArcGIS Pro Menu navigate to the Python section and install `Boto3` package into
your virtual environment.

## Run

1. Open the toolbox in ArcMap or ArcGIS Pro and select the Forest Loss Analysis tool.
2. Select the input feature for which you want to run the analysis.
3. Select the tree cover density threshold for which you want to compute the analysis.
You can select  more than one threshold.
4. Select if you want to include Primary Forest and/or Plantations in your analysis.
This will dis-aggregate loss by the selected layers.
You will end up with multiple rows per feature and tree cover densisty threshold.]
5. Analysis of the forest carbon flux model is selected by default. This will provide
   gross emissions, gross removals and net carbon flux from the forest carbon flux model
   (Harris et al. 2021) instead of the simple emissions from biomass lost. This will include 
   annual gross emissions, total gross emissions across all modeled years, total gross 
   removals across all modeled years, and total net flux across all modeled years, 
   in addition to the data that this client provides by default. 
   The output csv folder on s3 will be called carbonflux_minimal_[DATESTAMP] instead of
   treecoverloss_[DATESTAMP].
6. Optionally, you can change the number of nodes for your EMR cluster. Default size is 1 master and 4 workers.

## Results

Once the analysis completes, your results will be storted on S3 (see path above).
Results are stored as CSV file. There will be one row per feature and selected
treecover density threshold.

The tool will you the object ID of the features as row identifier.

To work with results in ArcMap/ ArcGIS Pro,
1. Download the CSV file
2. Open it in ArcMap/ ArcGIS Pro
3. Optionally, apply a definition query for your tree cover density threshold if you selected more than one.
4. Join you feature class with the CSV file using the object ID.

