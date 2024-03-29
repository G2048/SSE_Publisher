from fastapi import Request


def get_headers(request: Request):
    return request.headers


def get_cookies(request: Request):
    return request.cookies


def get_producer(request: Request):
    return request.state.producer


def get_topic(request: Request):
    return request.state.topic
