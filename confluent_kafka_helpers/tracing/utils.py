def decode_headers(headers: dict) -> dict:
    return {k: v.decode('utf-8') for k, v in dict(headers).items()}
