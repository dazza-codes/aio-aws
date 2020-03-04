class A:
    def __init__(self, x: int):
        self.x = x

    def foo(self, y: int):
        return self.x * y


def test_a_foo(mocker):
    a = A(1)
    y = -10
    assert a.foo(y) < 0
    mocker.patch.object(a, "foo", return_value=a.x * abs(y))
    assert a.foo(y) > 0


class B(A):
    def foo(self, y: int):
        if y < 0:
            return self.x * abs(y)
        else:
            super().foo(y)


def test_b_foo(mocker):
    a = A(1)
    y = -10
    assert a.foo(y) < 0
    mocker.patch.object(A, "foo", side_effect=B(a.x).foo)
    assert a.foo(y) > 0
