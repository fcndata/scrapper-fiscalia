from trigger import main

def lambda_handler(event, context : None ) -> dict:
    context = 
    try:
        df = main()
        
        return {"statusCode": 200, "body": str(df.head(1))}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
