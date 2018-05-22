
source .env.local

source .env.local

sed -i 1 "s/AWS::REGION/$AWS_DEFAULT_REGION/g" swagger.yaml
sed -i 2 "s/AWS::ACCOUNT_ID/$AWS_ACCOUNT_ID/g" swagger.yaml
sed -i 2 "s/AWS::CUSTOM_AUTHORIZER_LAMBDA_NAME/$CUSTOM_AUTHORIZER_LAMBDA_NAME/g" swagger.yaml
sed -i 2 "s/AWS::CUSTOM_AUTHORIZER_ROLE_NAME/$CUSTOM_AUTHORIZER_ROLE_NAME/g" swagger.yaml
cd src; npm install; cd ..

aws cloudformation package \
   --template-file ./templates/template.yaml \
   --s3-bucket $S3_BUCKET_NAME \
   --output-template-file samTemplate.yaml


mv swagger.yaml1 swagger.yaml
rm swagger.yaml2


aws cloudformation deploy --template-file ./samTemplate.yaml \
  --capabilities CAPABILITY_IAM \
  --stack-name SungardAS-aws-services-billing \
  --parameter-overrides RedshiftUser=$REDSHIFT_USER RedshiftPass=$REDSHIFT_PASS \
  RedshiftDatabase=$REDSHIFT_DATABASE VpcCidr=$VPC_CIDR PublicCidr1=$PUBLIC_CIDR_1 PublicCidr2=$PUBLIC_CIDR_2 \
  PrivateCidr1=$PRIVATE_CIDR_1 PrivateCidr2=$PRIVATE_CIDR_2 NameTag=$NAME_TAG AlarmThresholdNumber=$ALARM_THRESHOLD_NUMBER \
  RedshiftSnapshotIdentifier=$REDSHIFT_SNAPSHOT_IDENTIFIER  RedshiftSnapshotClusterIdentifier=$REDSHIFT_SNAPSHOT_CLUSTER_IDENTIFIER \
  AllowedAverageCost=$ALLOWED_AVERAGE_COST PlotlyUsername=$PLOTLY_USER_NAME PlotlyAPIKey=$PLOTLY_API_KEY \
  BillingLogGroupName=$BILLING_LOG_GROUP_NAME SubscriptionFilterDestinationArn=$SUBSCRIPTION_FILTER_DESTINATION_ARN

rm samTemplate.yaml
