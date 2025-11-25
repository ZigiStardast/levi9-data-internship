from my_utils import hello


def handler(event, context):
    """
    Demo Lambda koja koristi layer.
    """
    name = event.get("name", "world")
    msg = hello(name)
    print(msg)
    return {"message": msg}
