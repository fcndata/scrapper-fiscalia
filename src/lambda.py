import json
from ..trigger import main
from logs.logger import logger


def lambda_handler(event, context) -> dict:
    try:
        df = main()
        # Tomamos la primera fila como lista de dicts
        payload = (
                    df.head(1)
                    .astype(str)  # convierte todos los valores a su representación de texto
                    .to_dict(orient="records"))
        
        print ({
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload)
        })

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload)
        }


    except Exception as e:
        logger.exception("Error en lambda_handler")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }


if __name__ == "__main__":
    test_event = {}
    test_context = None
    lambda_handler(test_event,test_context)

