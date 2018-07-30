class Foo:
    def __bool__(self):
        return False

    def __eq__(self, other):
        return True

    def __repr__(self):
        return ''


f = Foo()
print(f is None)
