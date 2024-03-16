from fastapi import Request


def get_producer(request: Request):
    return request.state.producer


def get_topic(request: Request):
    return request.state.topic
