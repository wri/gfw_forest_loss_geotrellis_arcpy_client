import arcpy
import binascii
import boto3
import itertools


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Tree Cover Loss Analysis Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [Tool]


class Tool(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Tool"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        # First parameter
        in_features = arcpy.Parameter(
            displayName="Input Features",
            name="in_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input",
        )

        tcd = arcpy.Parameter(
            displayName="Tree Cover Density Threshold",
            name="tcd",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
           # default=[30],
            multiValue=True,
        )
        # Set a value list of 1, 10 and 100

        tcd.filter.type = "ValueList"
        tcd.filter.list = list(range(0, 100, 5))
        tcd.value = 30

        slack_user = arcpy.Parameter(
            displayName="Slack user name",
            name="slack_user",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
			category="Notifications",
        )

        slack_user.filter.type = "ValueList"
        slack_user.filter.list = ["David Gibbs", "liz.goldman", "thai", "thomas"]

        nb_workers = arcpy.Parameter(
            displayName="Number of workers",
            name="nb_workers",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
            category="Spark config",
        )

        nb_workers.value = 4

        instance_type = arcpy.Parameter(
            displayName="Instance Parameter",
            name="instance_parameter",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            category="Spark config",
        )

        instance_type.filter.type = "ValueList"
        instance_type.list = ["m3.2xlarge"]
        instance_type.value = "m3.2xlarge"

        out_features = arcpy.Parameter(
            displayName="Out features",
            name="out_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Output")

        out_features.value = r"in_memory\out_features"

        params = [in_features, tcd, slack_user, nb_workers, instance_type, out_features]

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        in_features = parameters[0].valueAsText
        arcpy.MakeFeatureLayer_management(in_features, "in_features")

        tcd = parameters[1].value

        sr = arcpy.SpatialReference(4326)
        fishnet_path = r"in_memory\fishnet"
        out_features_path = parameters[5].valueAsText

        # Create fishnet
        messages.addMessage("Compute in memory fishnet")
        arcpy.CreateFishnet_management(
            fishnet_path,
            "-180 -90",
            "-180, 90",
            1,
            1,
            180,
            360,
            labels="NO_LABELS",
            template=arcpy.Extent(-180, -90, 180, 90),
            geometry_type="POLYGON",
        )
        messages.addMessage("Define Projection for Fishnet")
        arcpy.DefineProjection_management(fishnet_path, sr)

        arcpy.MakeFeatureLayer_management(fishnet_path, "fishnet")

        # intersect input feature with fishnet and forest loss extent
        messages.addMessage("Load Loss Extent")
        loss_extent_geom = arcpy.AsShape(self.loss_extent, False)
        arcpy.CreateFeatureclass_management("in_memory" , "loss_extent", "POLYGON", spatial_reference=sr)

        cursor = arcpy.da.InsertCursor(r"in_memory\loss_extent", ["SHAPE@"])
        cursor.insertRow([loss_extent_geom])

      #  messages.addMessage("Define Projection for Loss Extent")
      #  arcpy.DefineProjection_management(loss_extent, sr)
		#
        arcpy.MakeFeatureLayer_management(r"in_memory\loss_extent", "loss_extent")
    #    arcpy.Copy_management(loss_proj,out_features_path)

        messages.addMessage("Intersect layers")
        arcpy.Intersect_analysis(
            in_features="in_features 3;loss_extent 1; fishnet 2",
            out_feature_class=out_features_path,
            join_attributes="ONLY_FID",
            cluster_tolerance="-1 Unknown",
            output_type="INPUT")

        # Export to WKB

        id_field = None
        fields = arcpy.ListFields(out_features_path, field_type = "Integer")
        for field in fields:
            if field.name != "FID_loss_extent" and field.name != "FID_fishnet":
                id_field = field.name

        with open(r"C:\ForestAtlas\outfile.tsv", "a+") as output_file:

            with arcpy.da.SearchCursor(
                r"in_memory\out_features", [id_field, "SHAPE@WKB"]
            ) as cursor:
                for row in cursor:
                    gid = row[0]
                    wkb = binascii.hexlify(row[1])
                    output_file.write(str(gid) + "\t" + wkb + "\n")

        # Upload inout features to S3

    #    s3 = boto3.resource("s3")
    #    s3.meta.client.upload_file(
    #        output_file, "gfw-files", "2018_updates/csv", "input_features.tsv"
    #    )

        # config spark cluster
        # start job
        # self._laucn_emr("S3://gfw-files/2018_updates/csv/input_features.tsv", tcd)

        return

    def _launch_emr(self, in_features, tcd, instance_type="m3.2xlarge", instance_count=20):

        client = boto3.client("emr")
        response = client.run_job_flow(
            Name="Geotrellis Forest Loss Analysis",
            LogUri="s3://gfw-files/2018_update/spark/logs",
            ReleaseLabel="emr-5.24.0",
            Instances={
                # "MasterInstanceType": instance_type,
                # "SlaveInstanceType": instance_type,
                # "InstanceCount": instance_count + 1,
                "InstanceGroups": [
                    {
                        "Name": "geotrellis-treecoverloss-master",
                        "Market": "ON_DEMAND",
                        "InstanceRole": "MASTER",
                        "InstanceType": instance_type,
                        "InstanceCount": 1,
                        "Configurations": [
                            {
                                "Classification": "string",
                                "Configurations": {"... recursive ..."},
                                "Properties": {"string": "string"},
                            }
                        ],
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp2",
                                        "SizeInGB": 10,
                                    },
                                    "VolumesPerInstance": 1,
                                }
                            ],
                            "EbsOptimized": True,
                        },
                    },
                    {
                        "Name": "geotrellis-treecoverloss-cores",
                        "Market": "SPOT",
                        "InstanceRole": "CORE",
                        "BidPrice": None,
                        "InstanceType": instance_type,
                        "InstanceCount": instance_count,
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp2",
                                        "SizeInGB": 10,
                                    },
                                    "VolumesPerInstance": 1,
                                }
                            ],
                            "EbsOptimized": True,
                        },
                    },
                ],
                "Ec2KeyName": "tmaschler2_wri",
                "Placement": {"AvailabilityZone": "us-east-1c"},
                "KeepJobFlowAliveWhenNoSteps": False,
                "TerminationProtected": False,
                "HadoopVersion": "string",
                "Ec2SubnetId": "string",
                "Ec2SubnetIds": ["subnet-08458452c1d05713b"],
                "EmrManagedMasterSecurityGroup": "subnet-08458452c1d05713b",
                "EmrManagedSlaveSecurityGroup": "subnet-08458452c1d05713b",
                "ServiceAccessSecurityGroup": "string",
                "AdditionalMasterSecurityGroups": [
                    "sg-d76cdbc1",
                    "sg-11e40a60",
                    "subnet-08458452c1d05713b",
                ],
                "AdditionalSlaveSecurityGroups": ["subnet-08458452c1d05713b"],
            },
            Steps=[
                {
                    "Name": "treecoverloss-analysis",
                    "ActionOnFailure": "TERMINATE_CLUSTER",
                    "HadoopJarStep": {
                        "Properties": [{"Key": "string", "Value": "string"}],
                        "Jar": "s3://gfw-files/2018_update/spark/jars/treecoverloss-assembly-0.8.4.jar",
                        "MainClass": "org.globalforestwatch.treecoverloss.TreeCoverLossSummaryMain",
                        "Args": [
                            "--features",
                            in_features,
                            "--output s3://gfw-files/2018_update/results",
                        ]
                        + list(zip(itertools.repeat("--threshold"), [i for i in tcd])),
                    },
                }
            ],
            Applications=[
                {"Name": "Spark", "Version": "2.4.2"},
                {"Name": "Zeppelin", "Version": "0.8.1"},
                {"Name": "Ganglia", "Version": "3.7.2"},
            ],
            Configurations=[
                {
                    "classification": "spark",
                    "properties": {"maximizeResourceAllocation": "true"},
                    "configurations": [],
                },
                {
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.executor.memory": "3G",
                        "spark.driver.memory": "3G",
                        "spark.driver.cores": "1",
                        "spark.driver.maxResultSize": "3G",
                        "spark.rdd.compress": "true",
                        "spark.executor.cores": "1",
                        "spark.sql.shuffle.partitions": "1390",
                        "spark.shuffle.spill.compress": "true",
                        "spark.shuffle.compress": "true",
                        "spark.default.parallelism": "1390",
                        "spark.shuffle.service.enabled": "true",
                        "spark.executor.extraJavaOptions": "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
                        "spark.executor.instances": "139",
                        "spark.yarn.executor.memoryOverhead": "1G",
                        "spark.dynamicAllocation.enabled": "false",
                        "spark.driver.extraJavaOptions": "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
                    },
                    "configurations": [],
                },
                {
                    "classification": "yarn-site",
                    "properties": {
                        "yarn.nodemanager.pmem-check-enabled": "false",
                        "yarn.resourcemanager.am.max-attempts": "1",
                        "yarn.nodemanager.vmem-check-enabled": "false",
                    },
                    "configurations": [],
                },
            ],
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            Tags=[
                {"Key": "Project", "Value": "Global Forest Watch"},
                {"Key": "Job", "Value": "Tree Cover Loss Analysis"},
            ],
        )
        return response

    loss_extent = {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [
                    [160, -50],
                    [160, -40],
                    [170, -40],
                    [170, -30],
                    [180, -30],
                    [180, -40],
                    [180, -50],
                    [170, -50],
                    [160, -50],
                ]
            ],
            [
                [
                    [-160, 10],
                    [-160, 20],
                    [-170, 20],
                    [-170, 30],
                    [-160, 30],
                    [-150, 30],
                    [-150, 20],
                    [-150, 10],
                    [-160, 10],
                ]
            ],
            [
                [
                    [140, -50],
                    [140, -40],
                    [130, -40],
                    [120, -40],
                    [110, -40],
                    [110, -30],
                    [110, -20],
                    [110, -10],
                    [100, -10],
                    [90, -10],
                    [90, 0],
                    [80, 0],
                    [70, 0],
                    [70, 10],
                    [70, 20],
                    [60, 20],
                    [60, 10],
                    [60, 0],
                    [50, 0],
                    [50, -10],
                    [60, -10],
                    [60, -20],
                    [60, -30],
                    [50, -30],
                    [40, -30],
                    [40, -40],
                    [30, -40],
                    [20, -40],
                    [10, -40],
                    [10, -30],
                    [10, -20],
                    [10, -10],
                    [0, -10],
                    [0, 0],
                    [-10, 0],
                    [-20, 0],
                    [-20, 10],
                    [-30, 10],
                    [-30, 20],
                    [-20, 20],
                    [-20, 30],
                    [-20, 40],
                    [-10, 40],
                    [-10, 50],
                    [-20, 50],
                    [-20, 60],
                    [-30, 60],
                    [-30, 70],
                    [-20, 70],
                    [-10, 70],
                    [0, 70],
                    [10, 70],
                    [10, 80],
                    [20, 80],
                    [30, 80],
                    [40, 80],
                    [40, 70],
                    [50, 70],
                    [50, 80],
                    [60, 80],
                    [70, 80],
                    [80, 80],
                    [90, 80],
                    [100, 80],
                    [110, 80],
                    [120, 80],
                    [130, 80],
                    [140, 80],
                    [150, 80],
                    [160, 80],
                    [170, 80],
                    [180, 80],
                    [180, 70],
                    [180, 60],
                    [180, 50],
                    [170, 50],
                    [160, 50],
                    [160, 40],
                    [150, 40],
                    [150, 30],
                    [140, 30],
                    [140, 20],
                    [130, 20],
                    [130, 10],
                    [140, 10],
                    [140, 0],
                    [150, 0],
                    [160, 0],
                    [170, 0],
                    [170, -10],
                    [180, -10],
                    [180, -20],
                    [170, -20],
                    [170, -30],
                    [160, -30],
                    [160, -40],
                    [150, -40],
                    [150, -50],
                    [140, -50],
                ]
            ],
            [
                [
                    [-80, -60],
                    [-80, -50],
                    [-80, -40],
                    [-80, -30],
                    [-80, -20],
                    [-80, -10],
                    [-90, -10],
                    [-100, -10],
                    [-100, 0],
                    [-100, 10],
                    [-110, 10],
                    [-110, 20],
                    [-120, 20],
                    [-120, 30],
                    [-130, 30],
                    [-130, 40],
                    [-130, 50],
                    [-140, 50],
                    [-150, 50],
                    [-160, 50],
                    [-170, 50],
                    [-180, 50],
                    [-180, 60],
                    [-180, 70],
                    [-170, 70],
                    [-170, 80],
                    [-160, 80],
                    [-150, 80],
                    [-140, 80],
                    [-130, 80],
                    [-120, 80],
                    [-110, 80],
                    [-100, 80],
                    [-90, 80],
                    [-80, 80],
                    [-70, 80],
                    [-60, 80],
                    [-60, 70],
                    [-60, 60],
                    [-50, 60],
                    [-50, 50],
                    [-50, 40],
                    [-60, 40],
                    [-60, 30],
                    [-70, 30],
                    [-70, 20],
                    [-60, 20],
                    [-50, 20],
                    [-50, 10],
                    [-40, 10],
                    [-40, 0],
                    [-30, 0],
                    [-30, -10],
                    [-30, -20],
                    [-40, -20],
                    [-40, -30],
                    [-50, -30],
                    [-50, -40],
                    [-60, -40],
                    [-60, -50],
                    [-50, -50],
                    [-50, -60],
                    [-60, -60],
                    [-70, -60],
                    [-80, -60],
                ]
            ],
        ],
    }
