class HelloWorld:
    """ HelloWorld class will tell you hello!

    """
    def __init__(self, firstname, lastname):
        """ initialize HelloWorld class

        Args:
           firstname (str): first name of user
           lastname (str): last name of user
        """
        self.firstname = firstname
        self.lastname = lastname

    @property
    def hello(self):
        """ say hello to user

        a longer message about what this function does

        Returns:
            str: special hello message to user
        """
        return f'Hello {self.firstname} {self.lastname}'

    def helloworld(self, name):
        """string with special hello message to name

        Args:
            name (str): the person to say hello to

        Returns:
            str: special hello world message
        """
        return f'Hello World {self.name}!'
