# modifications by Jude Nelson

from SMDS.method import Method
from SMDS.parameter import Parameter

class methodHelp(Method):
    """
    Returns help text if defined for the method passed, otherwise
    returns an empty string.
    """

    roles = []
    accepts = [Parameter(str, 'Method name')]
    returns = Parameter(str, 'Method help')

    def __init__(self, api):
        Method.__init__(self, api)
        self.name = "system.methodHelp"

    def call(self, method):
        function = self.api.callable(method)
        return function.help()
