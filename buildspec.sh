
source .env.local

cd src; npm install; cd ..

aws cloudformation package \
   --template-file ./templates/template.yaml \
   --s3-bucket $S3_BUCKET_NAME \
   --output-template-file samTemplate.yaml


aws cloudformation deploy --template-file ./samTemplate.yaml \
  --capabilities CAPABILITY_IAM \
  --stack-name SungardAS-aws-services-billing-us-east-2 \
  --parameter-overrides RedshiftUser=$REDSHIFT_USER RedshiftPass=$REDSHIFT_PASS \
  RedshiftDatabase=$REDSHIFT_DATABASE VpcCidr=$VPC_CIDR PublicCidr1=$PUBLIC_CIDR_1 PublicCidr2=$PUBLIC_CIDR_2 \
  PrivateCidr1=$PRIVATE_CIDR_1 PrivateCidr2=$PRIVATE_CIDR_2 NameTag=$NAME_TAG \
  RedshiftSnapshotIdentifier=$REDSHIFT_SNAPSHOT_IDENTIFIER  RedshiftSnapshotClusterIdentifier=$REDSHIFT_SNAPSHOT_CLUSTER_IDENTIFIER

rm samTemplate.yaml
