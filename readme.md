# Forest Loss Analysis

This is an ArcPy Python Toolbox which preprocesses vector data and launches
GFW Tree Cover Loss Analysis on AWS EMR/ Spark from within ArcMap or ArcGIS Pro.
You can select your feature class in ArcMap/ ArcPro. The toolbox will preprocess your data,
and split features into smaller chunks for better partitioning. It then export your data
as TSV file and uploads it to S3.
Afterwards it launches a SPARK cluster on AWS EMR and runs the GFW tree cover analysis.
The last step is asynchronous. The toolbox exits while the cluster is still running.

You can monitor progress directly on AWS EMR console. Final results will be stored on S3

`s3://gfw-files/2018-update/result/treecoverloss_<date_time>/`

## Installation and dependencies
You need a licensed version of ArcMap or ArcGIS Pro to run the toolbox. The toolbox itself
uses BOTO3 to communicate with AWS. You will need to install this package into your Python
installation.

__ArcMap__

Open the Command Line Tool with Administrator rights and run:

`C:\Python27\ArcGIS10.X\scripts\pip.exe install boto3`
Make sure you select the correct Python version which correspond with your ArcGIS version number.


__ArcGIS Pro__

In ArcGIS Pro Menu navigate to the Python section and install `Boto3` package into
your virtual environment.

