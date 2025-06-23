from trigger import main

def lambda_handler(event, context):
    try:
        df = main()
        return df
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
