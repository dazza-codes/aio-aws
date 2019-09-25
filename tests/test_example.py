from package.hello import HelloWorld, fizzbuzz


def test_helloworld_hello():
    helloworld = HelloWorld("Fred", "Bloggs")
    assert helloworld.hello == "Hello Fred Bloggs"


def test_helloworld_helloworld():
    assert HelloWorld.helloworld("Fred") == "Hello World Fred!"


def test_fizzbuzz(capsys):
    fizzbuzz(9)
    captured = capsys.readouterr()
    assert captured.out == "1\n2\nFizz\n4\nBuzz\nFizz\n7\n8\nFizz\n"
