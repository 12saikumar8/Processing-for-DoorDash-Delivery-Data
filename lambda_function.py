import json
import boto3
import pandas as pd 

def lambda_handler(event,context):
    #Intialize s3 and Sns clients
    
    s3_client=boto3.client('s3')
    sns_client=boto3.client('sns')
    sns_arn=arn:aws:sns:us-east-1:730335181268:Sns-for-doordash
    
    # Define the S3 bucket and file key for the source JSON file
    bucket_name='doordash-landing-zn-sai'
    key='2024-03-09-raw_input.json'
    
    # Define the S3 bucket and file key for the target JSON file
    target_bucket_name = 'doordash-target-zn-sai'
    target_key = 'output.json'
    
    try:
        response=s3_client.get_object(Bucket=bucket_name,key=key)
        data=json.loads(response['Body'].read())
        df=pd.DataFrame(data)
        
         # Filter records where status is "delivered"
        filtered_df = df[df['status'] == 'delivered']
        
        # Convert filtered DataFrame to JSON format   
        json_data=filtered_df.to_json(oreint='records')
        
        
        # Write filtered DataFrame to a new JSON file in the target S3 bucket
        s3_client.put_object(Bucket=target_bucket_name,key=target_key,Body=json_data)
        
         # Publish success message to SNS topic
        sns_client.publish(
            TopicArn=sns-arn,
            Subject='Lambda Function Execution Success',
            Message='Filtered records successfully written to JSON file in S3.'
        )
        
    except Exception as e:
        # publish failure message to sns_topic
        sns_client.publish(
            TopicArn=sns_arn,
            Subject='Lambda Function Execution Failed'
            Message=f'Error: {str(e)}'
        )
        
        raise e
        
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function execution completed successfully!')
    }
        
        
        
        
