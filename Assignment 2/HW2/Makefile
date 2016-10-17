######## Declaring variables ############################################################
initial.jar=target/HW2-0.0.1-SNAPSHOT.jar	# the location of the jar after make. 
jar.Name=hw2.jar				# new name which will be given to the jar 	
noCombiner.Class=com.neu.cs6240.hw2.NoCombiner	# class Name for the No Combiner solution
Combiner.Class=com.neu.cs6240.hw2.Combiner	# class name for the combiner solution
InMapCombiner.Class=com.neu.cs6240.hw2.InMapCombiner #class name for In Map solution
secSort.Class=com.neu.cs6240.hw2.SecSort	# class name for Secondary sort
aws.bucket.name=ankitssunnyhw2			# Name by which aws s3 buket will be created
local.input.directory=/home/ankit/hw2_inputs/secSortInput/ # The directory location for the files for Secondary sort solution
local.input=/home/ankit/hw2_inputs/1991.csv	# The file path to 1991.csv
local.output=/home/ankit/Testoutput		# Location where output files will be written for local run
aws.output=/home/ankit/awsOutput			
aws.input=input
aws.output.noCombiner=outputNoCombiner		# Output folder name in S3 for No combiner run
aws.log.noCombiner=logNoCombiner		# Log folder name in S3 for No combiner run
aws.output.Combiner=outputCombiner		# Output folder name in S3 for Combiner run
aws.log.combiner=logCombiner			# Log folder name in S3 for Combiner run
aws.output.InMapCombiner=outputInMapCombiner	# Output folder name in s3 for in map run
aws.log.inMapCombiner=logInMapCombiner		# Log folder name in S3 for in map run
aws.output.secSort=outputSecSort		# Output folder name in S3 for Secondary sort		
aws.log.secSort=logSecSort			# Log folder name in S3 for Secondary sort	
aws.num.nodes=5					# Number of slaves to be used in EMR	
aws.instance.type=m4.large			# Instance type of slaves
aws.subnet.id=subnet-bb79f2e3			# subnet ID to be used during EMR run.	
aws.region=us-west-2				# region to be used during EMR run
aws.profile=--profile ankitssunny		# My profile id. I used this method of aws cli authentication
#########################################################################################

###### Compiles code and builds jar (with dependencies) #################################
default: 
	mvn clean package
	cp  ${initial.jar} ${jar.Name}

# Remove the old jar File
remove-jar:
	rm ${jar.Name}

# Removes local output directory.
delete-output:
	rm -rf ${local.output}
#########################################################################################

##### Defining makes for local run ######################################################

# Run NoCombiner on local Machine
local-noCombiner: delete-output
	hadoop jar ${jar.Name} ${noCombiner.Class} ${local.input} ${local.output}

# Run Combiner on local Machine
local-Combiner: delete-output
	hadoop jar ${jar.Name} ${Combiner.Class} ${local.input} ${local.output}

# Run In Map Combiner on local Machine
local-inMapCombiner: delete-output
	hadoop jar ${jar.Name} ${InMapCombiner.Class} ${local.input} ${local.output}

# Run Secondary Sort on local Machine
local-secSort: delete-output
	hadoop jar ${jar.Name} ${secSort.Class} ${local.input.directory} ${local.output}
#########################################################################################

############## Defining for AWS S3 upload ###############################################

# Create S3 bucket.
s3-create:
	aws s3 ${aws.profile} mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
s3-upload-input:
	aws s3 ${aws.profile} cp ${local.input} s3://${aws.bucket.name}/${aws.input}/

# Upload data to S3 input dir for Secondary Sort.
s3-upload-input-secSort:
	aws s3 ${aws.profile} sync ${local.input.directory} s3://${aws.bucket.name}/${aws.input}/

# Delete S3 bucket.
s3-remove:
	aws s3 ${aws.profile} rb s3://${aws.bucket.name} --force

# Upload application to S3 bucket.
s3-upload-app:
	aws s3 ${aws.profile} cp ${jar.Name} s3://${aws.bucket.name}

# Creates bucket, uploads input file and uploads the jar to work with on S3
s3-all: s3-create s3-upload-input s3-upload-app

# Creates bucket, uploads input file and uploads the jar to work with on S3
s3-all-secSort: s3-create s3-upload-input-secSort s3-upload-app

#########################################################################################

#### AWS EMR run for No Combiner and copying the S3 output and log to local disk ########
# Main EMR launch
emr-noCombiner:
	aws emr ${aws.profile} create-cluster \
		--name "ankitssunnyhw2" \
		--release-label emr-5.0.0 \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output.noCombiner}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.Name}","MainClass":"${noCombiner.Class}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.noCombiner} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Download output from S3 for No Combiner.
download-output-noCombiner:
	mkdir ${aws.output.noCombiner}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.output.noCombiner} ${aws.output.noCombiner}
	mkdir ${aws.log.noCombiner}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.log.noCombiner} ${aws.log.noCombiner}
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.log.noCombiner} --recursive
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.output.noCombiner} --recursive
#########################################################################################

#### AWS EMR run for Combiner and copying the S3 output and log to local disk ###########
# Main EMR launch
emr-combiner:
	aws emr ${aws.profile} create-cluster \
		--name "ankitssunnyhw2" \
		--release-label emr-5.0.0 \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output.Combiner}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.Name}","MainClass":"${Combiner.Class}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.combiner} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Download output from S3 for Combiner.
download-output-Combiner:
	mkdir ${aws.output.Combiner}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.output.Combiner} ${aws.output.Combiner}
	mkdir ${aws.log.combiner}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.log.combiner} ${aws.log.combiner}
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.log.combiner} --recursive
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.output.Combiner} --recursive
#########################################################################################

#### AWS EMR run for In Mapper Combiner and copying the S3 output and log to local disk #####
# Main EMR launch
emr-inMapCombiner:
	aws emr ${aws.profile} create-cluster \
		--name "ankitssunnyhw2" \
		--release-label emr-5.0.0 \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output.InMapCombiner}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.Name}","MainClass":"${InMapCombiner.Class}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.inMapCombiner} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Download output from S3 for Combiner.
download-output-inMapCombiner:
	mkdir ${aws.output.InMapCombiner}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.output.InMapCombiner} ${aws.output.InMapCombiner}
	mkdir ${aws.log.inMapCombiner}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.log.inMapCombiner} ${aws.log.inMapCombiner}
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.log.inMapCombiner} --recursive
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.output.InMapCombiner} --recursive
#########################################################################################

#### AWS EMR run for In Mapper Combiner and copying the S3 output and log to local disk #####
# Main EMR launch
emr-secSort: 
	aws emr ${aws.profile} create-cluster \
		--name "ankitssunnyhw2" \
		--release-label emr-5.0.0 \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output.secSort}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.Name}","MainClass":"${secSort.Class}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.secSort} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Download output from S3 for Combiner.
download-secSort:
	mkdir ${aws.output.secSort}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.output.secSort} ${aws.output.secSort}
	mkdir ${aws.log.secSort}
	aws s3 ${aws.profile} sync s3://${aws.bucket.name}/${aws.log.secSort} ${aws.log.secSort}
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.log.secSort} --recursive
	aws s3 ${aws.profile} rm s3://${aws.bucket.name}/${aws.output.secSort} --recursive
#########################################################################################
