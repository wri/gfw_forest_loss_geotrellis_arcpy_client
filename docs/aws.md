# AWS resources and permissions

This script depends on several resources that have been set up in WRI's AWS account and requires that the user running the script have the correct permissions.


## Permissions [in process]

Users who want to run this script should be added to the appropriate user groups.

1. `hadoop-users`: This is the main set of permissions required. It provides the access needed for EMR to run. 
2. `wri-s3-user`: This gives users the ability to write the output to their user folder on s3.
3. `gfw-users`: May be needed to provide access to s3://gfw-data-lake but that is TBD.


## AWS resources

The required resources are defined in [TreeCoverLossAnalysis.pyt#L371-L576](../TreeCoverLossAnalysis.pyt#L371-L576). Nothing should have to be done with these to use this script. This is just to provide some documentations for AWS admins.
