# modifications by Jude Nelson

from SMDS.method import Method
from SMDS.parameter import Parameter
import SMDS.Methods

class listMethods(Method):
    """
    This method lists all the methods that the XML-RPC server knows
    how to dispatch.
    """

    roles = []
    accepts = []
    returns = Parameter(list, 'List of methods')

    def __init__(self, api):
        Method.__init__(self, api)
        self.name = "system.listMethods"

    def call(self):
        return self.api.all_methods
