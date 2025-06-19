from trigger import main

def lambda_handler(event, context):
    try:
        main()
        return {"statusCode": 200, "body": "Scraping ejecutado correctamente"}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
