import arcpy
import binascii
import boto3
from datetime import datetime
import itertools
import os


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Tree Cover Loss Analysis Toolbox"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [TreeCoverLossAnalysis]


class TreeCoverLossAnalysis(object):
    out_features_path = None  # r"in_memory\out_features"
    fishnet_path = r"in_memory\fishnet"
    loss_extent_path = r"in_memory\loss_extent"
    tsv_path = os.getenv("LOCALAPPDATA")
    tsv_file = None  # "treecoverloss.tsv"
    tsv_fullpath = None  # os.path.join(tsv_path, tsv_file)
    s3_in_folder = "geotrellis/input_features"
    s3_out_folder = "geotrellis/results"
    s3_log_folder = "geotrellis/logs"
    s3_bucket = "wri-users"
    sr = arcpy.SpatialReference(4326)

    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Tree Cover Loss Analysis"
        descript_1 = "Tree Cover Loss Analysis running on AWS EMR/Geotrellis. "
        descript_2 = "Flux model results (emissions, removals, net) are for (TCD>X OR Hansen gain 2000-2020=TRUE OR mangrove=TRUE NOT pre-2000 plantations). "
        descript_3 = "Non-flux model results (total area, biomass stock/density, tree cover extent, gain and loss area) are for (TCD>X)."
        self.description = descript_1 + descript_2 + descript_3
        self.canRunInBackground = False
        self.aws_identity_label = (
            boto3.client("sts").get_caller_identity().get("Arn").split("/")[-1].split("@")[0]
        )
        self.s3_in_features_prefix = "{}/{}".format(
            self.aws_identity_label, self.s3_in_folder
        )

    def getParameterInfo(self):
        """Define parameter definitions"""

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
            multiValue=True,
        )

        tcd.filter.type = "ValueList"
        tcd.filter.list = list(range(0, 100, 5))
        tcd.value = 30

        tcd_year = arcpy.Parameter(
            displayName="Tree Cover Density Reference Year",
            name="tcd_year",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
        )

        tcd_year.filter.type = "ValueList"
        tcd_year.filter.list = [2000, 2010]
        tcd_year.value = 2000

        primary_forests = arcpy.Parameter(
            displayName="Humid tropical primary forest 2001 (Turubanova et al. 2018)",
            name="primary_forests",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Contextual layers: results by...",
        )
        primary_forests.value = False

        intact_forests = arcpy.Parameter(
            displayName="Intact forest landscapes 2000 (Potapov et al. 2017)",
            name="intact_forests",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Contextual layers: results by...",
        )
        intact_forests.value = False

        plantations = arcpy.Parameter(
            displayName="Planted trees (Spatial Database of Planted Trees v2.0)",
            name="plantations",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Contextual layers: results by...",
        )
        plantations.value = False

        global_peat = arcpy.Parameter(
            displayName="Global peat (aggregated by GFW for carbon flux model, 2023)",
            name="global_peat",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Contextual layers: results by...",
        )
        global_peat.value = False

        tree_cover_loss_drivers = arcpy.Parameter(
            displayName="Driver of tree cover loss (1km model: Sims et al. 2025, with TCL through 2024)",
            name="tree_cover_loss_drivers",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Contextual layers: results by...",
        )
        tree_cover_loss_drivers.value = False

        tree_cover_loss_from_fires = arcpy.Parameter(
            displayName="Tree cover loss from fires (Tyukavina et al. 2022, updated through 2024)",
            name="tree_cover_loss_from fires",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Contextual layers: results by...",
        )
        tree_cover_loss_from_fires.value = False

        is__umd_tree_cover_loss = arcpy.Parameter(
            displayName="Presence/absence of tree cover loss (updated through 2024)",
            name="is__umd_tree_cover_loss",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Contextual layers: results by...",
        )
        is__umd_tree_cover_loss.value = False

        carbon_pools = arcpy.Parameter(
            displayName="Include aboveground, belowground, and soil carbon 2000 stock analyses",
            name="carbon_pools",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Carbon options",
        )
        carbon_pools.value = False

        simple_AGB_emissions = arcpy.Parameter(
            displayName="Output timeseries of emissions from aboveground biomass loss (old emissions model, pre-Harris et al. 2021)",
            name="simple_AGB_emissions",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Carbon options",
        )
        simple_AGB_emissions.value = False

        emissions_by_gas_annually = arcpy.Parameter(
            displayName="Output timeseries of emissions from CO2, CH4, and N2O separately (from Harris et al. 2021, updated through 2024)",
            name="emissions_by_gas_annually",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
            category="Carbon options",
        )
        emissions_by_gas_annually.value = False

        master_instance_type = arcpy.Parameter(
            displayName="Master Instance Type",
            name="master_instance_type",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            category="Spark config",
        )

        master_instance_type.filter.type = "ValueList"
        master_instance_type.filter.list = ["r5.2xlarge", "m5.4xlarge", "c5.9xlarge"]
        master_instance_type.value = "r5.2xlarge"

        worker_instance_type = arcpy.Parameter(
            displayName="Worker Instance Type",
            name="worker_instance_type",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            category="Spark config",
        )

        worker_instance_type.filter.type = "ValueList"
        worker_instance_type.filter.list = ["r4.2xlarge", "r5.2xlarge"]
        worker_instance_type.value = "r5.2xlarge"

        instance_count = arcpy.Parameter(
            displayName="Number of workers",
            name="instance_count",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
            category="Spark config",
        )

        instance_count.value = 4

        jar_version = arcpy.Parameter(
            displayName="JAR version",
            name="jar_version",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            category="Spark config",
        )

        jar_version.value = "2.4.17_flux-model-v1-4-2-run"

        out_features = arcpy.Parameter(
            displayName="Out features",
            name="out_features",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Output",
        )

        out_features.value = r"in_memory\treecoverloss_{}".format(
            datetime.now().strftime("%Y%m%d%H%M%S")
        )

        add_features_to_map = arcpy.Parameter(
            displayName="Add features to map",
            name="add_features_to_map",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
        )
        add_features_to_map.value = False

        params = [
            in_features,
            tcd,
            tcd_year,
            primary_forests,
            intact_forests,
            plantations,
            global_peat,
            tree_cover_loss_drivers,
            tree_cover_loss_from_fires,
            is__umd_tree_cover_loss,
            carbon_pools,
            simple_AGB_emissions,
            emissions_by_gas_annually,
            master_instance_type,
            worker_instance_type,
            instance_count,
            jar_version,
            out_features,
            add_features_to_map,
        ]

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

        arcpy.env.overwriteOutput = True
        arcpy.env.outputCoordinateSystem = self.sr

        in_features = parameters[0].valueAsText
        tcd = parameters[1].values
        tcd_year = parameters[2].value
        primary_forests = parameters[3].value
        intact_forests = parameters[4].value
        plantations = parameters[5].value
        global_peat = parameters[6].value
        tree_cover_loss_drivers = parameters[7].value
        tree_cover_loss_from_fires = parameters[8].value
        is__umd_tree_cover_loss = parameters[9].value
        carbon_pools = parameters[10].value
        simple_AGB_emissions = parameters[11].value
        emissions_by_gas_annually = parameters[12].value
        master_instance_type = parameters[13].value
        worker_instance_type = parameters[14].value
        worker_instance_count = parameters[15].value
        jar_version = parameters[16].valueAsText

        self.out_features_path = parameters[17].valueAsText
        add_features_to_map = parameters[18].value

        self.tsv_file = os.path.basename(self.out_features_path) + ".tsv"
        self.tsv_fullpath = os.path.join(self.tsv_path, self.tsv_file)

        arcpy.MakeFeatureLayer_management(in_features, "in_features")

        self._make_fishnet_layer(messages)
        self._make_loss_extent_layer(messages)
        self._chop_geometries(messages)
        if add_features_to_map:
            self._load_layer(messages)
        self._export_wbk(messages)
        self._upload_to_s3(messages)
        self._launch_emr(
            "s3://{}/{}/{}".format(
                self.s3_bucket, self.s3_in_features_prefix, self.tsv_file
            ),
            tcd,
            tcd_year,
            primary_forests,
            intact_forests,
            plantations,
            global_peat,
            tree_cover_loss_drivers,
            tree_cover_loss_from_fires,
            is__umd_tree_cover_loss,
            carbon_pools,
            simple_AGB_emissions,
            emissions_by_gas_annually,
            master_instance_type,
            worker_instance_type,
            worker_instance_count,
            jar_version,
            messages,
        )
        self._clean_up(add_features_to_map, messages)

        messages.addMessage(
            "DONE - check AWS EMR for cluster status and AWS S3 folder for results"
        )

        return

    def _make_fishnet_layer(self, messages):

        messages.addMessage("Compute 1x1 degree fishnet")
        arcpy.CreateFishnet_management(
            self.fishnet_path,
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
        arcpy.DefineProjection_management(self.fishnet_path, self.sr)
        arcpy.MakeFeatureLayer_management(self.fishnet_path, "fishnet")

    def _make_loss_extent_layer(self, messages):
        messages.addMessage("Load Loss Extent")
        loss_extent_geom = arcpy.AsShape(self.loss_extent, False)
        arcpy.CreateFeatureclass_management(
            "in_memory", "loss_extent", "POLYGON", spatial_reference=self.sr
        )
        cursor = arcpy.da.InsertCursor(self.loss_extent_path, ["SHAPE@"])
        cursor.insertRow([loss_extent_geom])
        arcpy.MakeFeatureLayer_management(self.loss_extent_path, "loss_extent")

    def _chop_geometries(self, messages):

        messages.addMessage("Intersect layers")
        arcpy.Intersect_analysis(
            in_features="in_features 3;loss_extent 1; fishnet 2",
            out_feature_class=self.out_features_path,
            join_attributes="ONLY_FID",
            cluster_tolerance="-1 Unknown",
            output_type="INPUT",
        )

    def _load_layer(self, messages):
        messages.addMessage("Add layer to map")
        mxd = arcpy.mapping.MapDocument("CURRENT")
        df = arcpy.mapping.ListDataFrames(mxd, "*")[0]
        layer = arcpy.mapping.Layer(self.out_features_path)
        arcpy.mapping.AddLayer(df, layer, "AUTO_ARRANGE")

    def _export_wbk(self, messages):

        messages.addMessage("Export to WKB")

        id_field = None
        fields = arcpy.ListFields(self.out_features_path, field_type="Integer")
        for field in fields:
            if field.name != "FID_loss_extent" and field.name != "FID_fishnet":
                id_field = field.name

        if os.path.exists(self.tsv_fullpath):
            os.remove(self.tsv_fullpath)

        with open(self.tsv_fullpath, "a+") as output_file:
            output_file.write("fid\tgeom\n")
            with arcpy.da.SearchCursor(
                self.out_features_path, [id_field, "SHAPE@WKB"]
            ) as cursor:
                for row in cursor:
                    gid = row[0]
                    wkb = binascii.hexlify(row[1])
                    output_file.write(str(gid) + "\t" + wkb.decode('utf-8') + "\n")

    def _upload_to_s3(self, messages):
        messages.addMessage("Upload to S3")
        s3 = boto3.resource("s3")
        s3.meta.client.upload_file(
            self.tsv_fullpath,
            self.s3_bucket,
            "{}/{}".format(self.s3_in_features_prefix, self.tsv_file),
        )

    def _launch_emr(
        self,
        in_features,
        tcd,
        tcd_year,
        primary_forests,
        intact_forests,
        plantations,
        global_peat,
        tree_cover_loss_drivers,
        tree_cover_loss_from_fires,
        is__umd_tree_cover_loss,
        carbon_pools,
        simple_AGB_emissions,
        emissions_by_gas_annually,
        master_instance_type,
        worker_instance_type,
        worker_instance_count,
        jar_version,
        messages,
    ):

        messages.addMessage("Start Cluster")
        client = boto3.client("emr", region_name="us-east-1")

        core_instance_count = 1
        if round(worker_instance_count/5) > 1:
            core_instance_count = int(round(worker_instance_count/5))

        task_instance_count = 1
        if round(worker_instance_count*4/5) > 1:
            task_instance_count = int(round(worker_instance_count*4/5))

        instances = {
            "InstanceGroups": [
                {
                    "Name": "geotrellis-treecoverloss-master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": master_instance_type,
                    "InstanceCount": 1,
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
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": worker_instance_type,
                    "InstanceCount": core_instance_count,
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
                    "Name": "geotrellis-treecoverloss-tasks",
                    "Market": "SPOT",
                    "InstanceRole": "TASK",
                    "InstanceType": worker_instance_type,
                    "InstanceCount": task_instance_count,
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
            "Ec2KeyName": "tmaschler_wri2",
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
            "Ec2SubnetIds": ["subnet-08458452c1d05713b"],
            "EmrManagedMasterSecurityGroup": "sg-093d1007a79ed4f27",
            "EmrManagedSlaveSecurityGroup": "sg-04abaf6838e8a06fb",
            "AdditionalMasterSecurityGroups": [
                "sg-d7a0d8ad",
                "sg-001e5f904c9cb7cc4",
                "sg-6c6a5911",
            ],
            "AdditionalSlaveSecurityGroups": ["sg-d7a0d8ad", "sg-6c6a5911"],
        }

        steps = [
            {
                "Name": "treecoverloss-analysis",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "cluster",
                        "--class",
                        "org.globalforestwatch.summarystats.SummaryMain",
                        "s3://gfw-pipelines/geotrellis/jars/treecoverloss-assembly-{}.jar".format(
                            jar_version
                        ),
                        "treecoverloss",
                        "--features",
                        in_features,
                        "--output",
                        "s3://{}/{}/{}".format(
                            self.s3_bucket, self.aws_identity_label, self.s3_out_folder
                        ),
                        "--tcd",
                        str(tcd_year),
                    ]
                    + [
                        item
                        for sublist in list(
                            map(
                                list,
                                zip(
                                    itertools.repeat("--threshold"),
                                    [str(i) for i in tcd],
                                ),
                            )
                        )
                        for item in sublist
                    ],
                },
            }
        ]

        if primary_forests:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--contextual_layer", "is__umd_regional_primary_forest_2001"]
            )
        if intact_forests:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--contextual_layer", "is__intact_forest_landscapes_2000"]
            )
        if plantations:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--contextual_layer", "is__gfw_plantations"]
            )
        if global_peat:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--contextual_layer", "is__global_peat"]
            )
        if tree_cover_loss_drivers:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--contextual_layer", "tcl_driver__class"]
            )
        if tree_cover_loss_from_fires:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--contextual_layer", "is__tree_cover_loss_from_fires"]
            )
        if is__umd_tree_cover_loss:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--contextual_layer", "is__umd_tree_cover_loss"]
            )

        if carbon_pools:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--carbon_pools"]
            )
        if simple_AGB_emissions:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--simple_agb_emissions"]
            )
        if emissions_by_gas_annually:
            steps[0]["HadoopJarStep"]["Args"].extend(
                ["--emissions_by_gas_annually"]
            )

        applications = [{"Name": "Spark"}, {"Name": "Zeppelin"}, {"Name": "Ganglia"}]

        configurations = [
            {
                "Classification": "spark",
                "Properties": {"maximizeResourceAllocation": "true"},
                "Configurations": [],
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.shuffle.spill.compress": "true",
                    "spark.yarn.appMasterEnv.AWS_REQUEST_PAYER": "requester",
                    "spark.yarn.appMasterEnv.GDAL_HTTP_MAX_RETRY":"10",
                    "spark.yarn.appMasterEnv.GDAL_HTTP_RETRY_DELAY":"10",
                    "spark.yarn.appMasterEnv.GDAL_MAX_DATASET_POOL_SIZE":"450",
                    "spark.driver.maxResultSize": "3G",
                    "spark.shuffle.compress": "true",
                    "spark.yarn.appMasterEnv.LD_LIBRARY_PATH": "/usr/local/miniconda/lib/:/usr/local/lib",
                    "spark.rdd.compress": "true",
                    "spark.shuffle.service.enabled": "true",
                    "spark.driver.defaultJavaOptions": "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
                    "spark.executorEnv.LD_LIBRARY_PATH": "/usr/local/miniconda/lib/:/usr/local/lib",
                    "spark.executorEnv.AWS_REQUEST_PAYER": "requester",
                    "spark.executorEnv.GDAL_HTTP_MAX_RETRY":"10",
                    "spark.executorEnv.GDAL_HTTP_RETRY_DELAY":"10",
                    "spark.executorEnv.GDAL_MAX_DATASET_POOL_SIZE":"450",
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.executor.defaultJavaOptions": "-XX:+UseParallelGC -XX:+UseParallelOldGC -XX:OnOutOfMemoryError='kill -9 %p'",
                },
                "Configurations": [],
            },
            {
                "Classification": "yarn-site",
                "Properties": {
                    "yarn.nodemanager.pmem-check-enabled": "false",
                    "yarn.resourcemanager.am.max-attempts": "1",
                    "yarn.nodemanager.vmem-check-enabled": "false",
                },
                "Configurations": [],
            },
            {
                "Classification": "emrfs-site",
                "Properties": {"fs.s3.useRequesterPaysHeader": "true"},
                "Configurations": [],
            },
        ]

        bootstrap_actions = [
            {
                "Name": "Install GDAL 3.8.3 dependencies",
                "ScriptBootstrapAction": {
                    "Path": "s3://gfw-pipelines/geotrellis/bootstrap/gdal-3.8.3.sh",
                    "Args": ["3.8.3"],
                },
            },
        ]

        response = client.run_job_flow(
            Name="Geotrellis Forest Loss Analysis",
            LogUri="s3://{}/{}/{}".format(
                self.s3_bucket, self.aws_identity_label, self.s3_log_folder
            ),
            ReleaseLabel="emr-6.3.0",
            Instances=instances,
            Steps=steps,
            Applications=applications,
            Configurations=configurations,
            BootstrapActions=bootstrap_actions,
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Tags=[
                {"Key": "Project", "Value": "Global Forest Watch"},
                {"Key": "Job", "Value": "Tree Cover Loss Analysis"},
            ],
        )

        messages.addMessage(response)
        return

    def _clean_up(self, keep_features, messages):
        messages.addMessage("Clean up")
        os.remove(self.tsv_fullpath)
        arcpy.Delete_management(self.fishnet_path)
        arcpy.Delete_management(self.loss_extent_path)
        if not keep_features:
            arcpy.Delete_management(self.out_features_path)

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
