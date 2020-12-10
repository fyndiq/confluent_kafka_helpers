class NullTracerBackend:
    __call__ = __getattr__ = lambda self, *_, **__: self

    def start_span(self, *args, **kwargs):
        return NullDecorator()

    def start_active_span(self, *args, **kwargs):
        return NullDecorator()

    def extract_headers_and_start_span(self, *args, **kwargs):
        return NullDecorator()

    def inject_headers_and_start_span(self, *args, **kwargs):
        return NullDecorator()


class NullDecorator:
    __enter__ = __getattr__ = lambda self, *_, **__: self

    def __call__(self, f, *args, **kwargs):
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    def __exit__(self, *args):
        pass
